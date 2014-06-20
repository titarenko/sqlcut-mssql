# mymssql

Opinionated wrapper around MS SQL client API.

# API

"Promises" means "returns promise".

## query(sql, paramsArray)

Promises array of results.

```js
db.query('select * from products where name = $1', ['beer']).then(console.log);
```

## querySingle(sql, paramsArray)

Promises first row of results.

## insert(tableName, record)

Promises identifier of record that is being inserted.

```js
db.insert('products', { name: 'beer' }).then(console.log);
```

## update(tableName, record)

Promises identifier of record that is being updated. Record must have identifier value.

```js
db.update('products', { id: 20, name: 'fish' }).then(console.log);
```

## remove(tableName, id)

Promises removal of record with given identifier from specified table.

```js
db.remove('products', 20).then(function () {
	console.log('Product 20 was removed');
});
```

## find(tableName, id)

Promises record with given identifier taken from specified table.

# License

BSD
