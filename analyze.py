from pymongo import MongoClient
from datetime import datetime, timezone, timedelta

now = datetime.utcnow().replace(tzinfo=timezone.utc)

db = MongoClient("localhost", 3269)
cursor = db.test.head.find({ 'updatetime': {
        '$gte': now - timedelta(hours=240),
        '$lt': now,
    }, })
documents = list(cursor)

cursor = db.test.documents.find()
source = list(cursor)

s = {x['name']:x for x in source}

from collections import defaultdict
def groupby(t, key, value=None):
    r = defaultdict(set)
    for x in t:
        k = key(x)
        if value: x = value(x)
        r[k].add(x)
    return r

import numpy as np
d = sorted(documents, key=lambda x:x['updatetime'])
# d = np.array(d)

len(d)
len(set((x['name'], x['updatetime'], x['reply']) for x in d))
g = groupby(documents, key=lambda x:x["name"], value=lambda x:(x['updatetime'], x["reply"]))
len(g)

# There are some extremely old post that appears on homepage.
lost = [x['name'] for x in source if x['name'] not in g.keys()]

from collections import OrderedDict
i = OrderedDict(sorted(g.items(), key=lambda x: -len(x[1])))

[s[x] for x in list(i.keys())[:20]]
