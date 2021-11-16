import boto3
from decimal import Decimal

from query_ddb import create_event_for_ts
from update_ddb import store_event

def lambda_handler(event, context):
    
    dynamodb = boto3.resource('dynamodb')

    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        ddb = record['dynamodb']
            
        # print(record['eventID'])
        # print(record['eventName'])
        # print("DynamoDB Record: " + json.dumps(ddb, indent=2))

        if (record['eventName'] in ['INSERT','MODIFY'] and 'sample_date' in ddb['Keys']):
            
            # fetch keys from ddb record
            sample_date = ddb['Keys']['sample_date']['S']
            sample_time = ddb['Keys']['sample_time']['S']
            
            # fetch device data from ddb record
            device_data = ddb['NewImage']['device_data']['M']
            
            # determine if a zeroing event has occurred and if it was a tare
            timestamp = Decimal(device_data['timestamp']['N'])
            weight = Decimal(device_data['weight']['N'])
            tare = device_data['tare']['BOOL']
            
            # print(timestamp)
            # print(weight)
            # print(tare)
            
            if (weight == 0 and not tare):
                print(f"Zero event detected: {sample_date} {sample_time} -> {timestamp}")
                
                # create an event to store for the zero record
                scale_event = create_event_for_ts(timestamp, dynamodb)
                print(scale_event)

                # store the event in dynamodb
                store_event(scale_event, dynamodb)


    return 'Successfully processed {} records.'.format(len(event['Records']))
