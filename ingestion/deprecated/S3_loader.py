from XML_parser import parse_gen
import boto3

s3 = boto3.client('s3')
#print(s3.list_buckets()['Buckets'])
test = s3.list_objects(Bucket='stackoverflow.insight')['Contents']
files = []
for item in test:
    files.append(item['Key'])

mo_posts = s3.get_object(Bucket='stackoverflow.insight', Key = 'mathoverflow/Posts.xml')
#print(files)
parse_gen = parse_gen(mo_posts['Body'])










