from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key, Attr
import argparse
import time
from decimal import *

def checktable(tname):
    region=boto3.session.Session().region_name
    dynamodb = boto3.resource('dynamodb', region_name=region) #low-level Client
    table = dynamodb.Table(tname) #define which dynamodb table to access

    print('Status of table is {}'.format(table.table_status))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("tablename", help="Name of the table to be checked")
    args = parser.parse_args()
    tnametocheck = (args.tablename) #section to collect argument from command line

    start = time.time()
    result = checktable(tnametocheck)
    end = time.time()
    print('Total time: {} sec'.format(end - start))
