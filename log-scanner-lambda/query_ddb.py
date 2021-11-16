import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone
import pytz as tz

PREVIOUS_TIME_INTERVAL = 30 * 60 * 1000  # 30 min

def timestamp_to_keys(timestamp):
    """ convert a timestamp to partition and sort keys """
    dt = datetime.utcfromtimestamp(int(timestamp/1000))

    partition_key = dt.strftime("%Y.%m.%d")
    sort_key = dt.strftime("%H:%M:%S:") + str(timestamp%1000).zfill(3)

    return (partition_key, sort_key)

def query_log(keys, dynamodb=None):
    """ query DynamoDB log table for entries matching keys """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cat_scale_log')
    response = table.query(
        KeyConditionExpression=Key('sample_date').eq(keys['pk'])
            & Key('sample_time').between(*keys['sk'])
    )
    return response['Items']

def fetch_log_entries_for_ts(ts, dynamodb=None):
    """ fetch all relevant log entries for an event """

    # calculate start timestamp from end
    start_ts = ts - PREVIOUS_TIME_INTERVAL
    end_ts = ts

    # compute keys to use for queries
    # may need two queries if over two days
    start_keys = timestamp_to_keys(start_ts)
    end_keys = timestamp_to_keys(end_ts)
    query_keys = []
    if (start_keys[0] == end_keys[0]):
        query_keys.append({ 'pk': end_keys[0], 'sk': [start_keys[1], end_keys[1]]})
    else:
        query_keys.append({ 'pk': start_keys[0], 'sk': [start_keys[1], '23:59:59.999']})
        query_keys.append({ 'pk': end_keys[0], 'sk': ['00:00:00.000', end_keys[1]]})

    # execute queries and concat results
    all_entries = []
    for query_key in query_keys:
        all_entries += query_log(query_key, dynamodb)

    # sort all entries by timestamp
    all_entries.sort(key=lambda x: x['device_data']['timestamp'], reverse=True)

    if (len(all_entries) > 0):

        # if first time stamp is zero skip it
        first_entry = all_entries[0]
        if first_entry['device_data']['weight'] == 0 and first_entry['device_data']['timestamp'] == ts:
            all_entries = all_entries[1:]

        else:
            return []

        # scan for next zero entry
        # zero_entry = None
        zero_idx = -1
        for idx, entry in enumerate(all_entries):
            if entry['device_data']['weight'] == 0:
                # zero_entry = entry
                zero_idx = idx
                break

        # fetch all values before zero index
        if (zero_idx > 0):
            return all_entries[:zero_idx]

        return all_entries

    return []


def create_event_for_ts(ts, dynamodb=None):
    """ create an event dict given a timestamp """

    entries = fetch_log_entries_for_ts(ts, dynamodb)

    if (len(entries) > 0):

        start_entry = entries[0]
        end_entry = entries[-1]

        # determine start and end time stamps for entries
        start_ts = start_entry['device_data']['timestamp']
        end_ts = end_entry['device_data']['timestamp']
        time_elapsed = round((start_ts - end_ts) / 1000, 1)

        # determine maximum weight during range
        max_weight_entry = max(entries, key=lambda x: x['device_data']['weight'])
        max_weight = max_weight_entry['device_data']['weight']

        # calculate local time data
        dt = datetime.utcfromtimestamp(int(end_ts/1000)).replace(tzinfo=timezone.utc)
        dt_local = dt.astimezone(tz.timezone('America/New_York'))
        date_local = dt_local.strftime("%Y.%m.%d")
        time_local = dt_local.strftime("%H:%M:%S:") + str(end_ts%1000).zfill(3)

        # return event data for timestamp
        return { 
                'timestamp': end_ts,
                'date_utc': end_entry['sample_date'],
                'time_utc': end_entry['sample_time'],
                'date_local': date_local,
                'time_local': time_local,
                'elapsed_sec': time_elapsed,
                'weight': max_weight,
            }
