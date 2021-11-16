import boto3

def store_event(event, dynamodb=None):
    """ create a new DynamoDB event record """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cat_scale_event')
    response = table.put_item(
        Item={
            'sample_date': event['date_utc'],
            'sample_time': event['time_utc'],
            'event_data': event
        }
    )
