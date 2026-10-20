import sys
sys.path.insert(0, r'e:/Lab 4.5/scratch/faust_pkg/faust-streaming-0.11.3')
from unittest.mock import Mock
from faust.serializers.registry import Registry
app = Mock()
app.conf.value_serializer = 'json'
reg = Registry(app=app, key_serializer='raw', value_serializer='json')
raw_json = b'{"event_id": "123", "amount": 500}'
print('Case 1: typ=bytes, serializer=None (what app.topic(value_type=bytes) does without value_serializer)')
try:
    res = reg.loads_value(bytes, raw_json, serializer=None)
    print('Result:', res)
except Exception as e:
    print('Error:', type(e), e)
print('\nCase 2: typ=bytes, serializer="raw" (with value_serializer="raw")')
try:
    res = reg.loads_value(bytes, raw_json, serializer='raw')
    print('Result:', type(res), res)
except Exception as e:
    print('Error:', type(e), e)