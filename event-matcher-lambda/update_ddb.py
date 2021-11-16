import boto3
from boto3.dynamodb.conditions import Key

def update_event(event, dynamodb=None):
    """ update DynamoDB event record with the new cat setting """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    event_data = event['event_data']

    table = dynamodb.Table('cat_scale_event')
    response = table.update_item(
        Key={
            'sample_date': event_data['date_utc'],
            'sample_time': event_data['time_utc'],
        },
        UpdateExpression="SET cat = :cat",
        ExpressionAttributeValues={
            ":cat": event['cat']
        }
    )

