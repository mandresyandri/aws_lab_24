from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key, Attr
import argparse
import time

def scan_movies(dname):
    region=boto3.session.Session().region_name
    dynamodb = boto3.resource('dynamodb', region_name=region) #low-level Client
    table = dynamodb.Table('movies') #define which dynamodb table to access

    recordcount = 0
    recordscannedcount = 0

    scanreturn = table.scan(                    # perform first scan
        FilterExpression=Attr("genre").eq(dname)
    )
    recordcount += scanreturn['Count']
    recordscannedcount += scanreturn['ScannedCount']

    while 'LastEvaluatedKey' in scanreturn.keys(): # if lastevaluatedkey is present, we need to keep scanning and adding to our counts until everything is scanned
        scanreturn = table.scan(
            FilterExpression=Attr("genre").eq(dname),
            ExclusiveStartKey = scanreturn['LastEvaluatedKey']
        )
        recordcount += scanreturn['Count']
        recordscannedcount += scanreturn['ScannedCount']

    return [recordcount, recordscannedcount]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("Genre", help="Search by genre, ex: Action... argument is case sensitive")
    args = parser.parse_args()
    query_direct = (args.Genre) #section to collect argument from command line

    start = time.time()
    movies = scan_movies(query_direct) #scan_movies returns our total counts as two items of a list
    end = time.time()
    print("Count is ", movies[0])   # print the count of items returned by the scan
    print("ScannedCount is ", movies[1])  # print the count of items that had to be scanned to process the scan
    print('Total time: {} sec'.format(end - start))
