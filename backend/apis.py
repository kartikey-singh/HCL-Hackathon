from __future__ import print_function
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from datetime import date
lis = []

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def initiate():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('NEWS_SUM')
    return table

def delete():
    table = initiate()
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'Unfiltered_text': each['Unfiltered_text'],
                }
            )
    print ('Success')

def enter(city, user, location, followers, following, unfiltered_text, text, popularity, topics, sentiment):
    global lis
    table = initiate()
    # utext = 'description'
    # text = 'text'
    # followers = 1000
    # following = 1000
    # popularity= 1
    # city = 'Delhi'
    # location = 'location'
    today = date.today().isoformat()
    # user = 'Ayush'
    # topics = 1
    Item={
            'City': city,
            'Unfiltered_text': unfiltered_text,
            'Text': text,
            'date': today,
            'Location': location,
            'User': user,
            'Followers': followers,
            'Following': following,
            'Popularity': popularity,
            'Topics': topics,
            'Sentiment': sentiment,
        }
    lis.append(Item)
    

def read(location):
    today = date.today().isoformat()
    table = initiate()
    response = table.scan(
        FilterExpression = Key('location').eq(location)
    )
    data = []
    for i in response['Items']:
        data.append(i)
    dump = {'data':data}
    return json.dumps(dump,indent=4, cls=DecimalEncoder)

# def update(user,rating):
#     table = initiate()
#     username = user
#     response = table.update_item(
#         Key={
#             'username': username
#         },
#         UpdateExpression = 'SET heading = :h',
#
#         ExpressionAttributeValues={
#             ':h': rating
#         },
#         ReturnValues="UPDATED_NEW"
#     )
#
#     print("UpdateItem succeeded:")
#     return json.dumps(response, indent=4, cls=DecimalEncoder)
