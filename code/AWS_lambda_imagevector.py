import json
import boto3
import urllib
import math
from boto3.dynamodb.conditions import Key, Attr

#happy, sad, calm, others, outdoor, s, v, child

list = [0,0,0,0,0,0,0,0]
Loud_C = [4.0, 1.0, 0.0, 5.0, 0.35, 0.04, 0.03]
Happy_C= [5.0, 0.0, 1.0, 4.0, 0.35, 0.03, 0.04]
Calm_C = [1.0, 3.0, 5.0, 0.0, 0.15, 0.06, 0.07]
Sad_C  = [0.0, 5.0, 3.0, 1.0, 0.15, 0.07, 0.06]
Else_C = [2.0, 3.0, 3.0, 2.5, 0.25, 0.05, 0.05]
vector = [(0.0, 0),(0.0, 1),(0.0, 2),(0.0, 3),(0.0, 4)]

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
recDict={}
Emotions = ''
Labels = ''

def get_s3_keys(bucket):
    obj = s3.list_objects_v2(Bucket=bucket)['Contents']
    [obj['Key'] for obj in sorted(obj, key=get_last_modified)]
    print (obj['Key'])
    return obj['Key']
    
    
def get_reko(key):
    table = dynamodb.Table('ImageData')

    resp = table.query(
        KeyConditionExpression = Key('ImageKey').eq(key)
    )


    if resp['Items']==[]: print('no item')
    else:
        recDict['Emotions'] = resp['Items'][0].values()[2]
        recDict['Labels'] = resp['Items'][0].values()[0]
        Emotions = resp['Items'][0].values()[2]
        Labels = resp['Items'][0].values()[0]
    
    a = Emotions.split() + Labels.split()
    list[0] = float(a[1])
    list[1] = float(a[3])
    list[2] = float(a[5])
    list[3] = float(a[7])
    list[4] = float(a[9])
    list[7] = float(a[11])
    
    recDict['Kid'] = int(list[7])
    
    print ('list:')
    print (list)
    
  
  
def get_vector():
    
    cluster=''
    for i in range(0,7):
        vector[0] = (vector[0][0] + list[i]*Loud_C[i], 0)
        vector[1] = (vector[1][0] + list[i]*Happy_C[i], 1)
        vector[2] = (vector[2][0] + list[i]*Calm_C[i], 2)
        vector[3] = (vector[3][0] + list[i]*Sad_C[i], 3)
        vector[4] = (vector[4][0] + list[i]*Else_C[i], 4)
        

    power = math.pow(vector[0][0],2) + math.pow(vector[1][0],2) + math.pow(vector[2][0],2) + math.pow(vector[3][0],2) + math.pow(vector[4][0],2)
    
    vector[0] = (vector[0][0]/math.sqrt(power) , 0)
    vector[1] = (vector[1][0]/math.sqrt(power) , 1)
    vector[2] = (vector[2][0]/math.sqrt(power) , 2)
    vector[3] = (vector[3][0]/math.sqrt(power) , 3)
    vector[4] = (vector[4][0]/math.sqrt(power) , 4)
    
    recDict['Vector'] = str(vector[0][0]) + ' ' + str(vector[1][0]) + ' ' + str(vector[2][0]) + ' ' + str(vector[3][0]) + ' ' + str(vector[4][0])
    
    cluster = sorted(vector)[4][1]
    print (vector)

    return cluster



    
def insert_vector(recDict):
    table = dynamodb.Table('ImageVector')
    table.put_item(
            Item={
                'ImageKey': recDict['ImageKey'],
                'cluster' : recDict['cluster'],
                'Vector': recDict['Vector'],
                'Kid' : recDict['Kid']
           }
    )
    
def insert_data():
    
    #print (recDict['Emotions'])
    #print (recDict['Labels'])
    #print (str(list[5]) + ' ' + str(list[6]))

    table2 = dynamodb.Table('ImageData')
    table2.put_item(
            Item={
                'ImageKey': recDict['ImageKey'],
                'Emotions' : recDict['Emotions'],
                'Labels': recDict['Labels'],
                'SV' : str(list[5]) + ' ' + str(list[6])
           }
    )

    
    
    

def lambda_handler(event, context):
    bucket = 'nowsic.hsv2'
    bucket_key = get_s3_keys(bucket)
    
    data = s3.get_object(Bucket=bucket, Key=bucket_key)
    json_data = data['Body'].read()
    
    a = json.loads(json_data)
    key = a["key"]  #image name
    list[5] = a["smean"] #s value
    list[6] = a["vmean"] #v value
    

    #key = 'Baby.jpg'
    recDict['ImageKey'] = key
    get_reko(key)
    
    
    try:

        recDict['cluster'] = get_vector()
        print(recDict)
        insert_vector(recDict)
        insert_data()
        

        
        bucket_1 = 'nowsic.hsv1'
        response = s3.delete_object(
            Bucket= bucket_1,
            Key= key,
            )
            
        bucket_2 = 'nowsic.hsv2'
        response1 = s3.delete_object(
            Bucket= bucket_2,
            Key= bucket_key,
            )
        
        
        #return response
    
    except Exception as e:
        print(e)
        raise e

        
    
