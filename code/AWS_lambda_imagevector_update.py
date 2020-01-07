import json
import boto3
import urllib
import math
from boto3.dynamodb.conditions import Key, Attr


Loud_C = [4.0, 1.0, 0.0, 5.0, 0.35, 0.04, 0.03]
Happy_C= [5.0, 0.0, 1.0, 4.0, 0.35, 0.03, 0.04]
Calm_C = [1.0, 3.0, 5.0, 0.0, 0.15, 0.06, 0.07]
Sad_C  = [0.0, 5.0, 3.0, 1.0, 0.15, 0.07, 0.06]
Else_C = [2.0, 3.0, 3.0, 2.5, 0.25, 0.05, 0.05]
vector = [(0.0, 0),(0.0, 1),(0.0, 2),(0.0, 3),(0.0, 4)]



dynamodb = boto3.resource('dynamodb')
recDict={}

  
def data_scan():
    table = dynamodb.Table('ImageData')
    response = table.scan(
    )
    for i in response['Items']:
        get_data(i)
        

#0-labels 1-key 2-SV 3-emo
def get_data(key):
    
    #happy, sad, calm, others, outdoor, s, v, child
    list = [0,0,0,0,0,0,0,0]
    ImageKey = key.values()[1]
    Emotions = key.values()[3]
    Labels = key.values()[0]
    SV = key.values()[2]
    
    a = Emotions.split() + Labels.split() + SV.split()
    
    list[0] = float(a[1])
    list[1] = float(a[3])
    list[2] = float(a[5])
    list[3] = float(a[7])
    list[4] = float(a[9])
    list[7] = float(a[11])
    list[5] = float(a[13])
    list[6] = float(a[14])
    
    #print (ImageKey)
    #print (list)
    kid = int(list[7])
    
    get_vector(ImageKey, list, kid)
    
    
def get_vector(ImageKey, list, kid):
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
    
    print (vector)
    cluster = sorted(vector)[4][1]
    
    recDict['Kid'] = kid
    recDict['cluster'] = cluster
    recDict['ImageKey'] = ImageKey
    
    print (recDict)
    
    update_vector(recDict)


def update_vector(recDict):
    table = dynamodb.Table('ImageVector')
    
    table.put_item(
            Item={
                'ImageKey': recDict['ImageKey'],
                'cluster' : recDict['cluster'],
                'Vector': recDict['Vector'],
                'Kid' : recDict['Kid']
           }
    )
    '''
    table.update_item(
        Key={
            'ImageKey': recDict['ImageKey']
            #'cluster' : recDict['cluster']
        },
        
        ProjectionExpression = "#c",
        ExpressionAttributeNames= { "#c", "cluster"},
        
        ExpressionAttributeValues={
            '#c' : recDict['cluster'],
            'Vector': recDict['Vector'],
            'Kid' : recDict['Kid']
        }
        
    )
    '''
    
    
    
def lambda_handler(event, context):
    data_scan()




  
  
