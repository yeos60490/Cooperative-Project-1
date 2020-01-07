from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib
from boto3.dynamodb.conditions import Key, Attr

print('Loading function')

rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
recDict = {}
lam = boto3.client('lambda')

# --------------- Helper Functions to call Rekognition APIs ------------------


def detect_faces(bucket, key):
    response = rekognition.detect_faces(Image={"S3Object": {"Bucket": bucket, "Name": key}}, Attributes=["DEFAULT","ALL"])
    return response['FaceDetails']


def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})
    print (response)
    return response
    
    
def index_faces(bucket, key):
    #rekognition.create_collection(CollectionId='BLUEPRINT_COLLECTION')
    response = rekognition.index_faces(Image={"S3Object": {"Bucket": bucket, "Name": key}}, 
                                                CollectionId="BLUEPRINT_COLLECTION", 
                                                DetectionAttributes=["DEFAULT","ALL"],
                                                ExternalImageId= key,
                                                MaxFaces=3)
    return response['FaceRecords']
    


def get_last_modified(object):
    return object['LastModified'] 


def get_s3_keys(bucket):
    obj = s3.list_objects_v2(Bucket=bucket)['Contents']
    a=[]

    for i in obj:
        a.append((get_last_modified(i), i['Key']))
        
    a = sorted(a)
    #sorted(a, key=lambda a:a[0])
    print (a[len(a)-1][1])
    return a[len(a)-1][1]
    
    


def emotion(bucket, key):
    resp = index_faces(bucket, key)
    p=1;
    a = [(0,'HAPPY'), (0,'SAD'), (0,'CALM'), (0,'Others')]
    o = [0,0,0,0]
    
    for faces in resp:
        list = []
        for emo in faces['FaceDetail']['Emotions']:
            for k,v in emo.items():
                list.append(v)
        #print (list)
        for i in range(0,7):
            if list[2*i] == 'HAPPY':
                a[0] = ((a[0][0]+list[2*i+1])/p , 'HAPPY')
            elif list[2*i]=='SAD': 
                a[1] = ((a[1][0]+list[2*i+1])/p , 'SAD')
            elif list[2*i]=='CALM': 
                a[2] = ((a[2][0]+list[2*i+1])/p , 'CALM')
                
            elif list[2*i]=='ANGRY': 
                o[0] = (o[0]+list[2*i+1])/p
            elif list[2*i]=='SURPRISED': 
                o[1] = (o[1]+list[2*i+1])/p
            elif list[2*i]=='DISGUSTED': 
                o[2] = (o[2]+list[2*i+1])/p
            else:
                o[3] = (o[3]+list[2*i+1])/p

        p = p+1
        
    o = sorted(o)    
    #if (p>1): a[3] = ( a[3][0]/((p-1)*4) , 'Others')
    a[3] = (o[len(o)-1], 'Others')

    
    string = 'face' + str(p-1)            
    for i in range(0,4):
        string = string + ' ' + str(a[i][0])+ ' ' + a[i][1]

    print (string)
    recDict['Emotions'] = string
    recDict['Labels'] = ' '
    





def insert_data(recDict):
    table = dynamodb.Table('ImageData')

    '''
    resp = table.query(
        KeyConditionExpression = Key('ImageKey').eq(recDict['ImageKey'])
    )
    
    if resp['Items']==[]: recDict['Labels'] = ' '
    else:
        recDict['Labels'] = resp['Items'][0].values()[0]
    
    print (resp['Items'])
    '''
    
    
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

        resp = emotion(bucket, key)

        insert_data(recDict)

        response = lam.invoke(FunctionName="imageRekoL", InvocationType='Event')
        
        #print (response)
        
        
        return resp
        

    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
        
