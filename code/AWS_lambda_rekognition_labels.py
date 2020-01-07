from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib
import os
from boto3.dynamodb.conditions import Key, Attr

print('Loading function')

rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')
sns = boto3.client('sns')
get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
dynamodb = boto3.resource('dynamodb')
recDict = {}


# --------------- Helper Functions to call Rekognition APIs ------------------


def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})
    return response


    
def get_s3_keys(bucket):
    obj = s3.list_objects_v2(Bucket=bucket)['Contents']
    [obj['Key'] for obj in sorted(obj, key=get_last_modified)]
    print (obj['Key'])
    return obj['Key']
    
    
    
def outdoors(bucket, key):
    resp = detect_labels(bucket, key)
    label = resp.values()[0]
    a = ['0 outdoors','0 kid']
    confi = 0

    for i in range(0,len(label)):
        if label[i].values()[1]=='Outdoors':
            print (label[i].values()[1])
            print (label[i].values()[0])
            confi = label[i].values()[0]
            a[0] = str(confi) + " outdoors"
        elif (label[i].values()[1]=='Kid' or label[i].values()[1]=='Baby'):
            print (label[i].values()[1])
            print (label[i].values()[0])
            confi = label[i].values()[0]
            a[1] = str(1) + " kid"
       
        
    #print (a)
    
    recDict['Labels'] = a[0]+' '+ a[1]
    #insert_data(recDict)
         
    return confi

            
def insert_data(recDict):
    table = dynamodb.Table('ImageData')

    resp = table.query(
        KeyConditionExpression = Key('ImageKey').eq(recDict['ImageKey'])
    )
    
    if resp['Items']==[]: recDict['Emotions'] = ' '
    else:
        recDict['Emotions'] = resp['Items'][0].values()[2]
    
    print (resp['Items'])
    
    table.put_item(
            Item={
                'ImageKey': recDict['ImageKey'],
                'Emotions': recDict['Emotions'],
                'Labels' : recDict['Labels']
           }
    )
    


# --------------- Main handler ------------------


def lambda_handler(event, context):
    
    bucket = "nowsicimage1"
    key = get_s3_keys(bucket)
    recDict['ImageKey'] = key

    try:
        
        response = outdoors(bucket, key)
        insert_data(recDict)
        print (recDict)
        
        #s3.upload_file(path, 'nowsic.hsv2', key)
        
        resp = s3.put_object(
            Bucket='nowsic.hsv1',
            Body='hi',
            Key=key,
            ServerSideEncryption='AES256'
            )
            
        

        return response


 
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
