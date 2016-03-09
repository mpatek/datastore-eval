import datetime
from glob import glob
import json
import os

from dateutil.parser import parse as parse_datetime
from dateutil.tz import tzlocal

DATA_DIR = '/home/mpatek/Downloads/mock-data-1752'


def yield_mock_data(data_dir=DATA_DIR):
    file_patt = os.path.join(data_dir, '*.json')
    for filename in glob(file_patt):
        data = json.loads(open(filename).read())
        filename = os.path.basename(filename)
        filename, _ = os.path.splitext(filename)
        fields = filename.split('__')
        if len(fields) == 3:
            obj_type, obj_id, end_time_str = fields
            end_time = parse_datetime(end_time_str).replace(
                tzinfo=tzlocal()
            )
        else:
            obj_type, obj_id = fields
            end_time = datetime.datetime.now(tzlocal())
        yield {
            'obj_type': obj_type,
            'obj_id': obj_id,
            'end_time': end_time,
            'data': data['data'],
        }
