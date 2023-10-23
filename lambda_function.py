import boto3
import json
import os
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    try:
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        response = table.query(
            IndexName = os.environ['POINTS_LSI_NAME'],
            ScanIndexForward=False,
            ProjectionExpression='TeamId,#n,Points',
            KeyConditionExpression=Key('PK').eq('Stage#'+event['queryStringParameters']['stage']),
            ExpressionAttributeNames = {'#n': 'Name'}
        )
        return {
            'statusCode': 200, 
            'body':json.dumps(json.loads(response['Items']))
        }
    except Exception as e:
        print(f'Exception: {e}')
    return {
            'statusCode': 500,
            'body': json.dumps('Invalid Request')
        }