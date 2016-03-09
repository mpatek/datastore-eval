import datetime

import rethinkdb

from mock_data import yield_mock_data

rethinkdb.connect('localhost', 28015).repl()

try:
    rethinkdb.db_drop('test').run()
except rethinkdb.errors.ReqlOpFailedError as e:
    pass

rethinkdb.db_create('test').run()
rethinkdb.db('test').table_create('timeseries').run()
rethinkdb.table('timeseries').index_create('end_time').run()
rethinkdb.table('timeseries').index_create('obj_type').run()
rethinkdb.table('timeseries').index_create('obj_id').run()

mock_data = list(yield_mock_data())
rethinkdb.table('timeseries').insert(mock_data).run()

div = '--------------------------------------------'

# get historical values for post_impressions_unique
# for video 1069936086365702_1343548795671095

print('Historical values for post_impressions_unique')
video_id = '1069936086365702_1343548795671095'
cursor = (
    rethinkdb.table('timeseries')
    .order_by(index='end_time')
    .filter(rethinkdb.row['obj_type'] == 'video')
    .filter(rethinkdb.row['obj_id'] == video_id)
    .run()
)
for doc in cursor:
    post_impressions_unique = [
        row['values'][0]['value']
        for row in doc['data']
        if row['name'] == 'post_impressions_unique'
    ][0]
    print(doc['end_time'], post_impressions_unique)

print(div)

# Get the most recent comment count for each video.
print('Most recent comment counts by video')
cursor = (
    rethinkdb.table('timeseries')
    .filter(rethinkdb.row['obj_type'] == 'video')
    .group('obj_id')
    .max('end_time')
    .run()
)
for video_id, doc in cursor.items():
    end_time = doc['end_time']
    comment_count = [
        row['values'][0]['value']['comment']
        for row in doc['data']
        if row['name'] == 'post_stories_by_action_type'
    ][0]
    print(video_id, end_time, comment_count)

print(div)

# Get the value of the page_fan_adds_unique
# metric for each page, for each date,
# starting from 2015-03-05.


def flatten_data(x):
    return x['data'].map(lambda row: {
        'obj_id': x['obj_id'], 'data': row
    })


def flatten_values(x):
    return x['data']['values'].map(lambda row: {
        'obj_id': x['obj_id'],
        'value': row['value'],
        'end_time': row['end_time'],
    })

start_date = datetime.date(2015, 3, 5).isoformat()

cursor = (
    rethinkdb.table('timeseries')
    .filter(rethinkdb.row['obj_type'] == 'page')
    .concat_map(flatten_data)
    .filter(lambda x: x['data']['name'] == 'page_fan_adds_unique')
    .concat_map(flatten_values)
    .filter(rethinkdb.row['end_time'] >= start_date)
    .group(lambda x: (x['obj_id'], x['end_time'].split('T')[0]))
    .max('end_time')
    .run()
)

for doc, item in cursor.items():
    print(doc, item)
