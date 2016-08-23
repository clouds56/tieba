var collections = db.getCollectionNames();
var result = {}
for (var i=0; i < collections.length; i++)
{
  result[collections[i]] = db[collections[i]].count()
}
print(JSON.stringify(result))
