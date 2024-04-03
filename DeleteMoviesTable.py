from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key, Attr
import argparse
import time
from decimal import *

def RemoveTable(tabletokill):
    region=boto3.session.Session().region_name
    dynamodb = boto3.resource('dynamodb', region_name=region) #low-level Client
    table = dynamodb.Table(tabletokill) #define which dynamodb table to access

    response = table.delete()  #command to delete the table

    return response

if __name__ == '__main__':

    result = RemoveTable("movies")  # call function to remove the movies table
    print (result)
