import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone
from decimal import Decimal
import pytz as tz

MILLIS_24_HOURS = 24 * 60 * 60 * 1000  # 24 hours
PREVIOUS_TIME_INTERVAL = 7 * MILLIS_24_HOURS  # 7 days
NUM_PREVIOUS_EVENTS = 3
OUTLIER_THRESHOLD = Decimal(0.55)

def query_settings_for_cats(dynamodb=None):
    """ query DynamoDB config table for cat entries """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cat_scale_settings')
    response = table.query(
        KeyConditionExpression=Key('setting_type').eq('cat-definition')
    )
    return response['Items']


def timestamp_to_keys(timestamp):
    """ convert a timestamp to partition and sort keys """
    dt = datetime.utcfromtimestamp(int(timestamp/1000))

    partition_key = dt.strftime("%Y.%m.%d")
    sort_key = dt.strftime("%H:%M:%S:") + str(timestamp%1000).zfill(3)

    return (partition_key, sort_key)

def query_events(keys, dynamodb=None):
    """ query DynamoDB log table for entries matching keys """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cat_scale_event')
    if 'sk' in keys:
        response = table.query(
            KeyConditionExpression=Key('sample_date').eq(keys['pk'])
                & Key('sample_time').between(*keys['sk'])
        )
    else:
        response = table.query(
            KeyConditionExpression=Key('sample_date').eq(keys['pk'])
        )

    return response['Items']


def fetch_cat_events_for_ts(cats, timestamp, dynamodb=None):
    """ fetch all relevant events for a timestamp """

    event_to_update = None

    # loop over days in reverse from starting timestamp up to limit interval
    start_ts = int(timestamp)
    end_ts = int(timestamp - PREVIOUS_TIME_INTERVAL)
    increment = int(-MILLIS_24_HOURS)
    for ts in range(start_ts, end_ts, increment):
        ts_keys = timestamp_to_keys(ts)

        # start by querying from start of day till the end key
        if (ts == timestamp):
            query_keys = { 'pk': ts_keys[0], 'sk': ['00:00:00.000', ts_keys[1]]}

        # subsequent iterations query the whole dah
        else:
            query_keys = { 'pk': ts_keys[0] }

        print(f"Query keys: {query_keys}")

        # fetch events matching query keys
        events = query_events(query_keys, dynamodb)

        # find the event to update if it is not defined
        if (not event_to_update):
            event_to_update = [e for e in events if e['event_data']['timestamp'] == ts][:1]

            # make sure events were found before proceeding
            if (len(event_to_update) == 0):
                return None
            event_to_update = event_to_update[0]    

        # loop over cats and find events relevant to them
        for cat in cats:
            cat_events = [ e for e in events if 'cat' in e and e['cat'] == cat['name'] ]

            if 'last_events' in cat:
                cat['last_events'] += cat_events
            else:
                cat['last_events'] = cat_events

        # determine if requisite number of events for each cat have been found
        # break out of loop and stop going backwards if this is the case
        smallest_num_events = min(len(cat['last_events']) for cat in cats)
        if (smallest_num_events >= NUM_PREVIOUS_EVENTS):
            break

    return event_to_update


def assign_cat_to_event(ts, dynamodb=None):

    # load settings of defined cats
    cats = query_settings_for_cats(dynamodb)

    # fetch event to update and relevant events for cats
    event_to_update = fetch_cat_events_for_ts(cats, ts, dynamodb)

    # make sure events were found before proceeding
    if not event_to_update:
        return None

    # determine last weight for each cat to use
    for cat in cats:

        # look at owner defined weights first for each cat
        # look for most recent weight defined that is before our queried timestamp
        defined_weights=[w for w in cat['defined_weights'] if w['timestamp'] <= ts]
        defined_weights.sort(key=lambda x: x['timestamp'], reverse=True)
        cat['configured_weight'] = defined_weights[0]['weight']
        cat['configured_weight_ts'] = defined_weights[0]['timestamp']

        # sort cat entries by timestamp then keep only the top previous events
        last_cat_events = cat['last_events']
        last_cat_events.sort(key=lambda x: x['event_data']['timestamp'], reverse=True)
        cat_events = [ 
                e for e in last_cat_events if e['event_data']['timestamp'] > cat['configured_weight_ts']
            ][:NUM_PREVIOUS_EVENTS]
        
        # use average of last event weights if possible, else use configured weight from owner
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
    event_weight = event_to_update['event_data']['weight']
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
