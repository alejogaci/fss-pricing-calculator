AWSTemplateFormatVersion: '2010-09-09'
Description: CloudFormation template for setting up AWS resources

Parameters:

  ControlTower:
    Type: String
    Description: Control Tower
    AllowedValues:
      - true
      - false

  MultiRegion:
    Type: String
    Description: Multi Region
    AllowedValues:
      - true
      - false

  TrailBucket:
    Type: String
    Description: Trail Bucket Name
    Default: filestorage-events-trendmicro
  AthenaBucket:
    Type: String
    Description: Athena Bucket Name
    Default: athena-results-trendmicro

  OrganizationID:
    Type: String
    Description: Organization ID


Conditions:
  IsMultiRegionTrailCondition: !Equals [!Ref MultiRegion, "true"]
  IsOrganizationTrailCondition: !Equals [!Ref ControlTower, "true"]

Resources:
  FileStorageBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Ref TrailBucket
      AccessControl: Private
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              SSEAlgorithm: AES256
      Tags:
        - Key: Name
          Value: FileStorageEventsBucket
          
  FileStorageBucketPolicy:
    Type: AWS::S3::BucketPolicy
    Properties:
      Bucket: !Ref FileStorageBucket
      PolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Sid: AWSCloudTrailAclCheck20150319
            Effect: Allow
            Principal:
              Service: cloudtrail.amazonaws.com
            Action: s3:GetBucketAcl
            Resource: !Sub "arn:aws:s3:::${FileStorageBucket}"
            Condition:
              StringEquals:
                "aws:SourceArn": !Sub "arn:aws:cloudtrail:${AWS::Region}:${AWS::AccountId}:trail/trendfilestorage"


          - Sid: AWSCloudTrailWrite20150319
            Effect: Allow
            Principal:
              Service: cloudtrail.amazonaws.com
            Action: s3:PutObject
            Resource: !Sub "arn:aws:s3:::${FileStorageBucket}/AWSLogs/${AWS::AccountId}/*"
            Condition:
              StringEquals:
                "s3:x-amz-acl": "bucket-owner-full-control"
                "aws:SourceArn": !Sub "arn:aws:cloudtrail:${AWS::Region}:${AWS::AccountId}:trail/trendfilestorage"

          - !If
            - IsOrganizationTrailCondition
            - Sid: AWSCloudTrailOrganizationWrite20150319
              Effect: Allow
              Principal:
                Service: cloudtrail.amazonaws.com
              Action: s3:PutObject
              Resource: !Sub "arn:aws:s3:::${FileStorageBucket}/AWSLogs/${OrganizationID}/*"
              Condition:
                StringEquals:
                  "s3:x-amz-acl": "bucket-owner-full-control"
                  "aws:SourceArn": !Sub "arn:aws:cloudtrail:${AWS::Region}:${AWS::AccountId}:trail/trendfilestorage"
            - !Ref "AWS::NoValue"


  AthenaResultsBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Ref AthenaBucket
      AccessControl: Private
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              SSEAlgorithm: AES256
      Tags:
        - Key: Name
          Value: AthenaResultsBucket


  CloudTrailTrail:
    Type: AWS::CloudTrail::Trail
    Properties:
      TrailName: trendfilestorage
      S3BucketName: !Ref FileStorageBucket
      IsLogging: true
      IsMultiRegionTrail: !If [IsMultiRegionTrailCondition, true, false]
      IsOrganizationTrail: !If [IsOrganizationTrailCondition, true, false]
      IncludeGlobalServiceEvents: true
      EventSelectors:
        - ReadWriteType: WriteOnly
          IncludeManagementEvents: false
          DataResources:
            - Type: "AWS::S3::Object"
              Values:
                - "arn:aws:s3:::"
    DependsOn: 
      - FileStorageBucketPolicy

  AppLogGroupCreateTable:
    Type: "AWS::Logs::LogGroup"
    Properties:
      LogGroupName: /aws/lambda/create-table-events

  CreateTableLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: "Create-FSSAthena-Table"
      Runtime: python3.8
      Timeout: 180
      MemorySize: 256
      Handler: index.lambda_handler
      Role: !GetAtt CreateTableLambdaRole.Arn
      Environment:
        Variables:
          TRAIL_BUCKET: !Ref TrailBucket
          ATHENA_BUCKET: !Ref AthenaBucket
      Code:
        ZipFile: |
          import boto3
          import time
          import json
          import logging
          import os
          import cfnresponse

          def lambda_handler(event, context):
              # Your Athena query code here
              athena_bucket = "s3://"+str(os.environ.get('ATHENA_BUCKET'))+"/logs/"
              athena_client = boto3.client('athena')
              database_name = 'fss_pricing_trend'
              table_name = 'cloudtrail_logs_fss_pricing'
              trail_bucketname = os.environ.get('TRAIL_BUCKET')
              if does_database_exist(database_name):
                  print(f"The database '{database_name}' exists in Athena.")
              else:
                  print(f"The database '{database_name}' does not exist in Athena.")
                  create_database(athena_bucket)
              account= get_account_id()
              if table_exists(database_name, table_name):
                  print(f"Table {table_name} exists in database {database_name}")
                  response_data = {"result": "success"}
                  cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
                  return {"statusCode": 200, "body": json.dumps("Thanks from Srce Cde!")}
              else:
                  print(f"Table {table_name} does not exist in database {database_name}")
              query="CREATE EXTERNAL TABLE cloudtrail_logs_fss_pricing (eventVersion STRING, userIdentity STRUCT<type: STRING, principalId: STRING, arn: STRING, accountId: STRING, invokedBy: STRING, accessKeyId: STRING, userName: STRING, sessionContext: STRUCT<attributes: STRUCT<mfaAuthenticated: STRING, creationDate: STRING>, sessionIssuer: STRUCT<type: STRING, principalId: STRING, arn: STRING, accountId: STRING, username: STRING>, ec2RoleDelivery: STRING, webIdFederationData: MAP<STRING,STRING>>>, eventTime STRING, eventSource STRING, eventName STRING, awsRegion STRING, sourceIpAddress STRING, userAgent STRING, errorCode STRING, errorMessage STRING, requestParameters STRING, responseElements STRING, additionalEventData STRING, requestId STRING, eventId STRING, resources ARRAY<STRUCT<arn: STRING, accountId: STRING, type: STRING>>, eventType STRING, apiVersion STRING, readOnly STRING, recipientAccountId STRING, serviceEventDetails STRING, sharedEventID STRING, vpcEndpointId STRING, tlsDetails STRUCT<tlsVersion: STRING, cipherSuite: STRING, clientProvidedHostHeader: STRING>) COMMENT 'CloudTrail table for my-cloudtrail-bucket bucket' ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 's3://"+trail_bucketname+"/AWSLogs/' TBLPROPERTIES ('classification'='cloudtrail');"
              database = "fss_pricing_trend"
              response = athena_client.start_query_execution(
                  QueryString=query,
                  QueryExecutionContext={
                      'Database': database
                  },
                  ResultConfiguration={
                      'OutputLocation': athena_bucket
                  }
              )

              query_execution_id = response['QueryExecutionId']
              print(f"Started Athena query with ID: {query_execution_id}")
              print("Creating Athena table...")

              while True:
                  finish_state = athena_client.get_query_execution(QueryExecutionId=query_execution_id)[
                      "QueryExecution"
                  ]["Status"]["State"]
                  if finish_state == "RUNNING" or finish_state == "QUEUED":
                      time.sleep(10)
                  else:
                      break
              assert finish_state == "SUCCEEDED", f"query state is {finish_state}"
              logging.info(f"Query {query_execution_id} complete")
              response_data = {"result": "success"}
              cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
              return {"statusCode": 200, "body": json.dumps("Thanks from Srce Cde!")}


          def get_account_id():
              try:
                  sts_client = boto3.client('sts')
                  response = sts_client.get_caller_identity()
                  account_id = response['Account']
                  return account_id
              except Exception as e:
                  print("Error:", e)
                  return None

          def table_exists(database_name, table_name):
              glue_client = boto3.client('glue')
              
              try:
                  response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
                  return True
              except glue_client.exceptions.EntityNotFoundException:
                  return False
                  
          def create_database(athena_bucket):
              ath = boto3.client('athena')
              
              response=ath.start_query_execution(
                  QueryString='create database fss_pricing_trend',
                  ResultConfiguration={'OutputLocation': athena_bucket})
                  

              query_execution_id = response['QueryExecutionId']
              print(f"Started Athena query with ID: {query_execution_id}")
              print("Creating Athena Database...")

              while True:
                  finish_state = ath.get_query_execution(QueryExecutionId=query_execution_id)[
                      "QueryExecution"
                  ]["Status"]["State"]
                  if finish_state == "RUNNING" or finish_state == "QUEUED":
                      time.sleep(10)
                  else:
                      break
              assert finish_state == "SUCCEEDED", f"query state is {finish_state}"
              logging.info(f"Query {query_execution_id} complete")
              
              
          def does_database_exist(database_name):
              # Create Athena client
              athena_client = boto3.client('athena')
              catalog_name = 'AwsDataCatalog'

              # List existing databases
              response = athena_client.list_databases(CatalogName=catalog_name)
              existing_databases = response.get('DatabaseList', [])

              # Check if the target database exists
              return any(db['Name'] == database_name for db in existing_databases)

          # Example usage
          target_database = 'fss_pricing_trend'


  CreateTableLambdaRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: CreateTableLambdaExecution
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action:
                  - athena:StartQueryExecution
                  - athena:GetQueryResults
                  - athena:GetQueryExecution
                  - athena:ListDatabases
                  - glue:GetDatabases
                  - glue:CreateTable
                  - glue:GetDatabase
                  - glue:GetTable
                  - glue:CreateDatabase
                Resource: '*'
              - Effect: Allow
                Action:
                  - s3:PutObject
                  - s3:GetObject
                  - logs:CreateLogStream
                  - s3:ListBucketMultipartUploads
                  - s3:AbortMultipartUpload
                  - s3:CreateBucket
                  - s3:ListBucket
                  - logs:CreateLogGroup
                  - logs:PutLogEvents
                  - s3:GetBucketLocation
                  - s3:ListMultipartUploadParts
                Resource:
                  - !Sub "arn:aws:s3:::${AthenaResultsBucket}"
                  - !Sub "arn:aws:s3:::${AthenaResultsBucket}/*"
                  - !Sub "arn:aws:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/create-table-events"

  CustomResource:
    Type: AWS::CloudFormation::CustomResource
    Properties:
      ServiceToken: !GetAtt CreateTableLambda.Arn

########################

  AppLogGroupData:
    Type: "AWS::Logs::LogGroup"
    Properties:
      LogGroupName: /aws/lambda/data-events

  DataLambdaFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: "Data_TrendMicro"
      Runtime: python3.8
      Handler: index.lambda_handler
      Timeout: 180
      Role: !GetAtt DataLambdaRole.Arn
      Environment:
        Variables:
          TRAIL_BUCKET: !Ref TrailBucket
          ATHENA_BUCKET: !Ref AthenaBucket
      Code:
        ZipFile: |
          import boto3
          import time
          import logging
          import io
          import csv
          import os


          def lambda_handler(event, context):
              # Your Athena query code here
              athena_client = boto3.client('athena')
              query = "SELECT COUNT(*) AS totalevent, eventname, SUBSTR(eventtime, 1, 13) AS eventhour, json_extract(requestparameters, '$.bucketName') AS bkt FROM cloudtrail_logs_fss_pricing WHERE eventname = 'PutObject' AND errorcode IS NULL GROUP BY eventname, json_extract(requestparameters, '$.bucketName'), SUBSTR(eventtime, 1, 13) ORDER BY eventhour"
              database = "fss_pricing_trend"
              result_bucket = "athena-results-trendmicro"
              athena_bucket = "s3://"+str(os.environ.get('ATHENA_BUCKET'))+"/logs/"
              excel_file_path = '/tmp/query_results.xlsx'

              response = athena_client.start_query_execution(
                  QueryString=query,
                  QueryExecutionContext={
                      'Database': database
                  },
                  ResultConfiguration={
                      'OutputLocation': athena_bucket
                  }
              )

              query_execution_id = response['QueryExecutionId']
              print(f"Started Athena query with ID: {query_execution_id}")

              # Wait for the Athena query to complete
              while True:
                  finish_state = athena_client.get_query_execution(QueryExecutionId=query_execution_id)[
                      "QueryExecution"
                  ]["Status"]["State"]
                  if finish_state == "RUNNING" or finish_state == "QUEUED":
                      time.sleep(10)
                  else:
                      break
              assert finish_state == "SUCCEEDED", f"query state is {finish_state}"
              logging.info(f"Query {query_execution_id} complete")


              # Get the results of the Athena query

              query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
              
              # Save query results to CSV file
              csv_file_content = []
              for row in query_results['ResultSet']['Rows']:
                  csv_file_content.append([field.get('VarCharValue', '') for field in row['Data']])
              
              s3 = boto3.client('s3')
              csv_filename = 'query_results.csv'
              csv_buffer = io.StringIO()
              csv_writer = csv.writer(csv_buffer)
              csv_writer.writerows(csv_file_content)
              s3.put_object(
                  Bucket=str(os.environ.get('ATHENA_BUCKET')),
                  Key='estadisticas/' + csv_filename,
                  Body=csv_buffer.getvalue().encode('utf-8')
              )

              return {
                  'statusCode': 200,
                  'body': 'Athena query results saved to CSV file in S3'
              }

  DataLambdaRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: LambdaBasicExecution
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Action:
                  - athena:StartQueryExecution
                  - athena:GetQueryExecution
                  - athena:GetQueryResults
                  - glue:GetTable
                Resource: '*'
                Effect: Allow
              - Action:
                  - s3:PutObject
                  - s3:GetBucketLocation
                  - s3:GetObject
                  - s3:ListBucket
                  - logs:CreateLogStream
                  - logs:CreateLogGroup
                  - logs:PutLogEvents
                Resource:
                  - !Sub "arn:aws:s3:::${AthenaResultsBucket}"
                  - !Sub "arn:aws:s3:::${AthenaResultsBucket}/*"
                  - !Sub "arn:aws:s3:::${FileStorageBucket}"
                  - !Sub "arn:aws:s3:::${FileStorageBucket}/*"
                  - !Sub "arn:aws:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/data-events"
                Effect: Allow

  DataEventRule:
    Type: AWS::Events::Rule
    Properties:
      ScheduleExpression: "rate(6 hours)"  # Schedule the Lambda function to run after 6 hours
      State: ENABLED
      Targets:
        - Arn: !GetAtt DataLambdaFunction.Arn
          Id: TargetFunctionV1
      Description: "Run Lambda function after 6 hours"
      EventPattern:
        source:
          - "aws.cloudformation"
        detail:
          eventName:
            - "CreateStack"

  PermissionForEventsToInvokeLambda: 
    Type: AWS::Lambda::Permission
    Properties: 
      FunctionName: 
        Ref: "DataLambdaFunction"
      Action: "lambda:InvokeFunction"
      Principal: "events.amazonaws.com"
      SourceArn: 
        Fn::GetAtt: 
          - "DataEventRule"
          - "Arn"  
