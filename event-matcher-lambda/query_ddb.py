import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone
from decimal import Decimal
import pytz as tz

PREVIOUS_TIME_INTERVAL = 24 * 60 * 60 * 1000  # 24 hours
NUM_PREVIOUS_EVENTS = 3
OUTLIER_THRESHOLD = Decimal(1.5)

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

    table = dynamodb.Table('cat_scale_event')
    response = table.query(
        KeyConditionExpression=Key('sample_date').eq(keys['pk'])
            & Key('sample_time').between(*keys['sk'])
    )
    return response['Items']

def fetch_last_events_for_ts(ts, dynamodb=None):
    """ fetch all relevant events for a timestamp """

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
        query_keys.append({ 'pk': start_keys[0], 'sk': [start_keys[1], '23:59:59.9999']})
        query_keys.append({ 'pk': end_keys[0], 'sk': ['00:00:00.0000', end_keys[1]]})

    # execute queries and concat results
    all_entries = []
    for query_key in query_keys:
        all_entries += query_log(query_key, dynamodb)

    # sort all entries by timestamp
    all_entries.sort(key=lambda x: x['event_data']['timestamp'], reverse=True)

    return all_entries


def query_settings_for_cats(dynamodb=None):
    """ query DynamoDB config table for cat entries """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cat_scale_settings')
    response = table.query(
        KeyConditionExpression=Key('setting_type').eq('cat-definition')
    )
    return response['Items']


def assign_cat_to_event(ts, dynamodb=None):

    # load settings of defined cats
    cats = query_settings_for_cats(dynamodb)

    # determine last weight for each cat to use
    for cat in cats:
        defined_weights = cat['defined_weights']
        defined_weights.sort(key=lambda x: x['timestamp'], reverse=True)
        cat['configured_weight'] = defined_weights[0]['weight']
        cat['configured_weight_ts'] = defined_weights[0]['timestamp']

    # fetch all the events in the last 24 hours
    events = fetch_last_events_for_ts(ts, dynamodb)
    # print(events)

    # locate event in question
    event_to_update = [e for e in events if e['event_data']['timestamp'] == ts][:1]
    
    # make sure events were found before proceeding
    if (len(event_to_update) == 0):
        return None
    event_to_update = event_to_update[0]

    event_weight = event_to_update['event_data']['weight']

    # loop over cats and find events relevant to them to determine weight to use
    for cat in cats:
        cat_events = [ 
            e for e in events 
            if 'cat' in e and e['cat'] == cat['name'] and e['event_data']['timestamp'] > cat['configured_weight_ts']
            ][:NUM_PREVIOUS_EVENTS]
        cat['last_events'] = cat_events

        if (cat_events):
            cat['last_weight'] = Decimal(sum(item['event_data']['weight'] for item in cat_events) / len(cat_events))            
        else:
            cat['last_weight'] = cat['configured_weight']

    # determine outlier values
    cat_weights = [cat['last_weight'] for cat in cats]
    min_weight = min(cat_weights)
    max_weight = max(cat_weights)
    low_outlier = min_weight - OUTLIER_THRESHOLD
    high_outlier = max_weight + OUTLIER_THRESHOLD

    # check if event is an outlier
    if event_weight < low_outlier:
        event_to_update['cat'] = 'OUTLIER_LOW'
        return event_to_update
    elif event_weight >= high_outlier:
        event_to_update['cat'] = 'OUTLIER_HIGH'
        return event_to_update

    # determine ranges for each cat
    cats.sort(key=lambda x: x['last_weight'])
    for idx, cat in enumerate(cats):
        this_cat_weight = cat['last_weight']

        # determine high weight value for cat
        if idx < len(cats)-1:
            next_cat_weight = cats[idx+1]['last_weight']
            high_weight = (next_cat_weight - this_cat_weight) / 2 + this_cat_weight
        else:
            high_weight = (high_outlier - this_cat_weight) / 2 + this_cat_weight

        # determine low weight value for cat
        if idx > 0:
            low_weight = cats[idx-1]['high_weight']  # high weight should have been determine for last cat, use it as the break here too
        else:
            low_weight = (this_cat_weight - low_outlier) / 2 + low_outlier

        # store range on cat
        cat['high_weight'] = high_weight
        cat['low_weight'] = low_weight

        print(f"Determined cat weight range for {cat['name']}: {low_weight} to {high_weight}")

        # check if event being updated falls in this cat's range
        if event_weight >= low_weight and event_weight < high_weight:
            event_to_update['cat'] = cat['name']
            return event_to_update

    # event is between outlier and top or bottom cat - assign as an outlier appropriately
    if event_weight < min_weight:
        event_to_update['cat'] = 'OUTLIER_LOW'
        return event_to_update
    elif event_weight >= max_weight:
        event_to_update['cat'] = 'OUTLIER_HIGH'
        return event_to_update

    # event is between cats - still an outlier
    event_to_update['cat'] = 'OUTLIER'
    return event_to_update

if __name__ == '__main__':

    dynamodb = boto3.resource('dynamodb')

    # ts = 1636940685633  # None

    ts = 1636978760263  # Mocha

    # ts = 1636999546290  # Latte

    # ts = 1637005825872 # Outlier

    event = assign_cat_to_event(ts)

    print(event)
