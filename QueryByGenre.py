from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key, Attr
import argparse
import time
from decimal import *

def MakeGSI():
    region=boto3.session.Session().region_name
    dynamodb = boto3.resource('dynamodb', region_name=region) #low-level Client
    table = dynamodb.Table('movies') #define which dynamodb table to access

    response = table.update(
        AttributeDefinitions=[
            {
                "AttributeName": "genre",
                "AttributeType": "S"
            },
        ],
        GlobalSecondaryIndexUpdates=[
            {
                'Create': {
                    'IndexName': "genre-globo-index",
                    'KeySchema': [
                        {
                            'AttributeName': "genre",
                            'KeyType': 'HASH'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 1000,
                        'WriteCapacityUnits': 1000
                    }
                }
            }
        ],
    )

    table.reload()       # this section to reload the table status every 10 seconds until the GSI finishes creating
    tmpreply = table.global_secondary_indexes
    indexnum = 0;
    while tmpreply[indexnum]['IndexName'] != "genre-globo-index" :   # in case other indexes exist for this table, find the one we just created
        indexnum += 1
    while tmpreply[indexnum]['IndexStatus'] != 'ACTIVE':    # check to see if the new one is active yet, and if not... repeat until it is
        time.sleep(10)
        print("Still creating...")
        table.reload()
        tmpreply = table.global_secondary_indexes

    return response

if __name__ == '__main__':

    start = time.time()
    result = MakeGSI()
    end = time.time()
    print('Total time: {} sec'.format(end - start))
