from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key, Attr
import argparse
import time
from decimal import *

def scan_movies(YearToFind,GenreToFind):
    region=boto3.session.Session().region_name
    dynamodb = boto3.resource('dynamodb', region_name=region) #low-level Client
    table = dynamodb.Table('movies') #define which dynamodb table to access

    recordcount = 0
    recordscannedcount = 0

    scanreturn = table.scan(                    # perform first scan
        FilterExpression=Key('year').eq(YearToFind) & Attr("genre").eq(GenreToFind)
    )
    recordcount += scanreturn['Count']
    recordscannedcount += scanreturn['ScannedCount']
    while 'LastEvaluatedKey' in scanreturn.keys(): # if lastevaluatedkey is present, we need to keep scanning and adding to our counts until everything is scanned
        scanreturn = table.scan(
            FilterExpression=Key('year').eq(YearToFind) & Attr("genre").eq(GenreToFind),
            ExclusiveStartKey = scanreturn['LastEvaluatedKey']
        )
        recordcount += scanreturn['Count']
        recordscannedcount += scanreturn['ScannedCount']
    return [recordcount, recordscannedcount]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("Qyear", help="Search by year and genre.. will return number of movies with that year and genre")
    parser.add_argument("Qgenre", help="Search by year and genre.. will return number of movies with that year and genre")
    args = parser.parse_args()
    queryyear = Decimal(args.Qyear)
    querygenre = (args.Qgenre) #section to collect argument from command line

    start = time.time()
    movies = scan_movies(queryyear, querygenre) #scan_movies returns our total counts as two items of a list
    end = time.time()
    print("Count is ", movies[0])   # print the count of items returned by the scan
    print("ScannedCount is ", movies[1])   # print the count of items that had to be scanned to process the scan
    print('Total time: {} sec'.format(end - start))
