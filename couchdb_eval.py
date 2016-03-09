import couchdb

from mock_data import yield_mock_data

couch = couchdb.Server()

couch.delete('test')
db = couch.create('test')

for doc in yield_mock_data():
    doc['end_time'] = doc['end_time'].isoformat()
    db.save(doc)


# List video docs? ugh.
map_fun = """
function(doc) {
    if (doc.obj_type == 'video')
        emit(doc, null);
}
"""
for row in db.query(map_fun):
    print(row.key)
