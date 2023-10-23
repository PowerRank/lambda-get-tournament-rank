import boto3
import os
import json
from dynamodb_json import json_util 
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    try:
        table = dynamodb.Table(os.environ['TABLE_NAME'])
        stageIds = []
        if event['queryStringParameters']['stage']:
            response = table.get_item(
                Key={
                    'PK':('Tournament#' + event['pathParameters']['tournament_id']),
                    'SK':('Stage#' + event['queryStringParameters']['stage'])
                },
                ProjectionExpression='StageId,#n',
                ExpressionAttributeNames={'#n': 'Name'}
            )
            stageIds.append([response['Item']['StageId'], response['Item']['Name']])
        else:
            response = table.query(
                ProjectionExpression='StageId,#n',
                KeyConditionExpression=Key('PK').eq('Tournament#' + event['pathParameters']['tournament_id']),
                ExpressionAttributeNames={'#n': 'Name'}
            )
            for item in response['Items']:
                stageIds.append([item['StageId'], item['Name']])
        ranks = {}
        for id in stageIds:
            response = table.query(
                IndexName=os.environ['POINTS_LSI_NAME'],
                ScanIndexForward=False,
                ProjectionExpression='TeamId,#n,Points',
                KeyConditionExpression=Key('PK').eq('Stage#'+id[0]),
                ExpressionAttributeNames={'#n': 'Name'}
            )
            ranks = ranks[id[1]]=response['Items']
        return {
            'statusCode': 200, 
            'body':json.dumps(json_util.loads(ranks))
        }
    except Exception as e:
        print(f'Exception: {e}')
    return {
            'statusCode': 500,
            'body': json.dumps('Invalid Request')
        }