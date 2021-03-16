import boto3

table_name = 'jg-pagerank'
region_name = 'us-west-2'
dynamodb = boto3.resource('dynamodb', region_name=region_name)
table = dynamodb.Table(table_name)

response = table.scan()

for res in response['Items']:
    print('data', res)