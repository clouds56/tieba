var collections = db.getCollectionNames();
var result = {};
var i;
for (i = 0; i < collections.length; i += 1) {
    result[collections[i]] = db[collections[i]].count();
}
print(JSON.stringify(result));
