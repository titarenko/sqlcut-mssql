var Q = require('q');
var _ = require('lodash');
var mssql = require('mssql');

var sprintf = require('util').format;

var deferredConnection;
var connectionPromise;

function connect () {
	if (connectionPromise) {
		return connectionPromise;
	}
	deferredConnection = Q.defer();
	var connection = new mssql.Connection(methods.uri, function (error) {
		if (error) {
			deferredConnection.reject(error);
		} else {
			deferredConnection.resolve(connection);
		}
	});
	return connectionPromise = deferredConnection.promise;
}

function renameNamedParameters (sql) {
	return sql.replace(/\$(\d+?)/g, function (m, g) {
		return '@param' + g;
	});
}

function nameDataProperties (data) {
	var result = {};
	for (var i in data) {
		result['param' + (i + 1)] = data[i];
	}
	return result;
}

function bindParameters (statement, data) {
	for (var i in data) {
		statement.input('param' + (i + 1), _.isNumber(data[i]) ? mssql.Int : mssql.NVarChar(2048));
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

var methods = {
	query: function (sql, params) {
		return connect().then(function (connection) {
			if (params) {
				return executePreparedStatement(connection, sql, params);
			} else {
				return executeRequest(connection, sql);
			} 
		});
	},

	querySingle: function (sql, params) {
		return methods.query(sql, params).then(function (rows) {
			return rows && rows.length >= 1 ? rows[0] : null;
		});
	},

	insert: function (table, data) {
		var sql = 'insert into %s (%s) output inserted.id values (%s)';

		var prepare = function(row) {
			var tokens = [];
			var count  = _.keys(row).length;

			for (var i = 1; i <= count; ++i) {
				tokens.push('?');
			}

			return tokens.join();
		};

		var fields = _.keys(data).join();
		var placeholders = prepare(data);
		
		sql = sprintf(sql, table, fields, placeholders);
		var params = _.values(data);

		return methods.query(sql, params).then(function (result) {
			return result[0].id;
		});
	},

	update: function(table, row) {
		if (!row.id) {
			throw new Error('Can\'t update row without id.');
		}

		var params = [];
		var sql = 'update ' + table + ' set %s where id = ?';
		
		var s = _.keys(row).filter(function (key) {
			return key !== 'id';
		}).map(function (key) {
			params.push(row[key]);
			return key + ' = ?';
		}).join(', ');

		params.push(row.id);
		sql = sprintf(sql, s);

		return methods.querySingle(sql, params).then(function () {
			return row.id;
		});
	},

	remove: function (table, id) {
		return methods.query('delete from ' + table + ' where id = $1', [id]);
	},

	find: function (table, id) {
		return methods.querySingle('select * from ' + table + ' where id = $1', [id]);
	}
};

try {
	require.resolve('../../config');
	methods.uri = require('../../config').db.uri;
} catch (e) {
	methods.uri = process.env.MSSQL_URI || '';
}

module.exports = methods;
