import boto3

def create_movie_table():
  region=boto3.session.Session().region_name
  dynamodb = boto3.resource('dynamodb', region_name=region) #low-level client
  table = dynamodb.create_table(
    TableName='movies',
    KeySchema=[
        {
            'AttributeName': 'year',
            'KeyType': 'HASH' #Partition Key
        },
        {
            'AttributeName': 'title',
            'KeyType': 'RANGE'  #Sort Key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'year',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'title',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'genre',
            'AttributeType': 'S'
        },
    ],
    LocalSecondaryIndexes=[
        {
            'IndexName': 'genre-index',
            'KeySchema': [
                {
                    'AttributeName': 'year',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'genre',
                    'KeyType': 'RANGE'
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL',
            }
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 1000,
        'WriteCapacityUnits': 1000
    }
  )
  return table

if __name__ == '__main__':
    movie_table = create_movie_table()
    print("Table status:", movie_table.table_status)
