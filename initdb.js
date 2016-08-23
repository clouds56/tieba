db.createCollection('documents', {
  validator: { $and:
    [
      { name: { $type: "string" } },
      { title: { $type: "string" } },
      { author: { $type: "string" } }
    ]
  }
})
db.createCollection('head', {
  validator: { $and:
    [
      { name: { $type: "string" } },
      { updatetime: { $type: "date" } },
      { reply: { $type: "int" } }
    ]
  }
})

print(db.getCollectionNames());
