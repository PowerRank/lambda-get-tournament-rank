import boto3
import json
import os
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def lambda_handler(event, context):
    response = table.query(
        IndexName = os.environ['POINTS_LSI_NAME'],
        ScanIndexForward=False,
        ProjectionExpression='TeamId,#n,Points',
        KeyConditionExpression=Key('PK').eq('Stage#'+event['stageId']),
        ExpressionAttributeNames = {'#n': 'Name'}
    )
    ranks = response['Items']
    for rank in ranks:
        rank['TeamId']=int(rank['TeamId'])
        rank['points']=float(rank['Points'])
    return {'statusCode': 200, 'body':json.dumps(ranks)}