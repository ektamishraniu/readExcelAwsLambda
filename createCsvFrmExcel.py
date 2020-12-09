import json
import boto3
import os
import csv
import psycopg2
import pandas as pd

s3c = boto3.client('s3')
s3r = boto3.resource('s3')


def PushCSVtoRedshiftTab(outCsvFile, mytable):
    print("working for file: "+srcBucket+"/"+outCsvFile)
    conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.cpbsfbtautsn.us-east-2.redshift.amazonaws.com', port='5439', user='ektamishraniu', password='Ashalini75')
    cur=conn.cursor()
    cur.execute("begin;")  
    delQuery = "delete from "+ mytable + ";" 
    cur.execute(delQuery)
    cur.execute("commit;")
    executeCom = "copy "+ mytable + " from 's3://" + srcBucket + "/" + outCsvFile + "' iam_role 'arn:aws:iam::686375945247:role/emRedShiftRole' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
    cur.execute(executeCom)
    cur.execute("commit;")

    cur.close()
    conn.close()
    return

def saveCsvFile(df, filterCols, outCsvFile):
    tmpAfile = "/tmp/" + outCsvFile
    Adf = df[filterCols]
    Adf.to_csv(tmpAfile , sep='|', index = False)
    s3r.Object(srcBucket, outCsvFile).put(Body=open(tmpAfile, 'rb'))
    return

def hms_to_s(s):
    t = 0
    for u in s.split(':'):
        t = 60 * t + int(u)
    return t
    
def workOnExcel():
    s3c.download_file(srcBucket, srcFile, '/tmp/' + srcFile) 
    infile = "/tmp/" + srcFile
    df = pd.read_excel(infile)
    os.remove(os.path.join(infile))
    #print(df.columns)
    print(df.columns.ravel())
    
    df = df.dropna()
    df['driver_id'] = df['driver_id'].astype(int)
    df['store_no'] = df['store_no'].astype(int) 
    df['service_time'] = df['service_time'].astype(str) 
    df['service_time'] = df['service_time'].apply(hms_to_s)  #Convert to seconds
    
    
    delivery_details = ['driver_id', 'service_time', 'route_date', 'store_no', 'item_cases', 'actual_arrival']
    outCsvFile = "delivery_details.csv"
    saveCsvFile(df, delivery_details, outCsvFile)
    PushCSVtoRedshiftTab(outCsvFile, "delivery_details")
    
    driver_details = ['driver_id', 'driver_first_name'] 
    outCsvFile = "driver_details.csv"
    saveCsvFile(df, driver_details, outCsvFile)
    PushCSVtoRedshiftTab(outCsvFile, "driver_details")    
    
    store_details = ['store_no', 'address_l2']
    outCsvFile = "store_details.csv"
    saveCsvFile(df, store_details, outCsvFile)
    PushCSVtoRedshiftTab(outCsvFile, "store_details")

    
    return


def lambda_handler(event, context):
    global srcBucket
    global srcFile
    srcBucket = "golden-state-foods"
    srcFile   = "POC-QLA_Sample_Data.xlsx"
    print("event: ", event)

    try:
        srcBucket = event['Records'][0]['s3']['bucket']['name']
        srcFile = event['Records'][0]['s3']['object']['key']
    except:
        print("Will be using: ", srcBucket, srcFile)
        
    tmpFile = '/tmp/' + os.path.basename(srcFile)

    print("fileName:     ", srcFile)    
    print("sourceBucket: ", srcBucket)
    print("tmpFile: ", tmpFile)

    workOnExcel()

    return