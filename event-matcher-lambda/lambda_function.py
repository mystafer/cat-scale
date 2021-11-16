import boto3
from decimal import Decimal

from query_ddb import assign_cat_to_event
from update_ddb import update_event

def determine_cat_and_assign(ts, dynamodb):
    event = assign_cat_to_event(ts, dynamodb)
    print(event)

    if (event):
        update_event(event, dynamodb)    

def lambda_handler(event, context):
    
    dynamodb = boto3.resource('dynamodb')

    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        ddb = record['dynamodb']
            
        # print(record['eventID'])
        # print(record['eventName'])
        # print("DynamoDB Record: " + json.dumps(ddb, indent=2))

        if (record['eventName'] in ['INSERT', 'MODIFY'] and 'sample_date' in ddb['Keys']):
            
            # check if cat already assigned before continuing
            if ('cat' in ddb['NewImage'] and 'S' in ddb['NewImage']['cat'] and ddb['NewImage']['cat']['S']):
                continue
            
            else:
                # fetch event data from ddb record
                event_data = ddb['NewImage']['event_data']['M']
                
                # locate timestamp to update event
                timestamp = Decimal(event_data['timestamp']['N'])
                print(timestamp)

                # perform update of event to assign a cat
                determine_cat_and_assign(timestamp, dynamodb)


    return 'Successfully processed {} records.'.format(len(event['Records']))


if __name__ == '__main__':

    dynamodb = boto3.resource('dynamodb')

    # ts = 1636940685633  # None

    ts = 1636978760263  # Mocha

    # ts = 1636999546290  # Latte

    # ts = 1637005825872 # Outlier

    determine_cat_and_assign(1637009836438, dynamodb)
