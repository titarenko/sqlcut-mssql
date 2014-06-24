var Q = require('q');
var _ = require('lodash');
var mssql = require('mssql');

function renameNamedParameters (sql) {
	var index = 1;
	return sql.replace(/\?/g, function () {
		return '@param' + index++;
	});
}

function nameDataProperties (data) {
	var result = {};
	for (var i in data) {
		result['param' + (i + 1)] = data[i];
	}
	return result;
}

function getMssqlType (value) {
	if (_.isNumber(value)) {
		return mssql.Decimal;
	} else if (_.isDate(value)) {
		return mssql.DateTime2;
	} else {
		return mssql.VarChar;
	}
}

function bindParameters (statement, data) {
	for (var i in data) {
		statement.input('param' + (i + 1), getMssqlType(data[i]));
	}
	return statement;
}

// Q.nbind doesn't work for mssql.PreparedStatement.execute method, 
// so we need to have this function
function getPreparedStatementExecuteMethod (statement) {
	return function (data) {
		var deferred = Q.defer();
		statement.execute(data, function (error, records) {
			if (error) {
				deferred.reject(error);
			} else {
				deferred.resolve(records);
			}
		});
		return deferred.promise;
	};
}

function executePreparedStatement (connection, sql, data) {
	var statement = new mssql.PreparedStatement(connection);

	sql = renameNamedParameters(sql);
	bindParameters(statement, data);
	data = nameDataProperties(data);

	var unprepare = Q.nbind(statement.unprepare, statement);

	return Q.ninvoke(statement, 'prepare', sql).then(function () {
		var execute = getPreparedStatementExecuteMethod(statement);
		return execute(data);
	}).then(function (rows) {
		return Q.ninvoke(statement, 'unprepare').then(function () {
			return rows;
		});
	});
}

function executeRequest (connection, sql) {
	var request = new mssql.Request(connection);
	return Q.ninvoke(request, 'query', sql);
}

function ctor (connectionParameters) {
	var deferredConnection;
	var connectionPromise;

	function connect () {
		if (connectionPromise) {
			return connectionPromise;
		}
		deferredConnection = Q.defer();
		var connection = new mssql.Connection(connectionParameters, function (error) {
			if (error) {
				deferredConnection.reject(error);
			} else {
				deferredConnection.resolve(connection);
			}
		});
		return connectionPromise = deferredConnection.promise;
	}
	
	return function (sql, params) {
		var isInsert = sql.indexOf('insert into') == 0;

		if (isInsert) {
			sql = sql.replace(') values (?', ') output inserted.id values (?');
		}

		var result = connect().then(function (connection) {
			if (params) {
				return executePreparedStatement(connection, sql, params);
			} else {
				return executeRequest(connection, sql);
			} 
		});

		if (isInsert) {
			result = result.then(function (rows) {
				return rows[0].id;
			});
		}

		return result;
	};
}

module.exports = ctor;
