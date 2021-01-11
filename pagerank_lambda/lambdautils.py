import boto3
import botocore
import os


class LambdaManager(object):
    def __init__(self, l, s3, region, codepath, fname, handler, lmem=1024):
        self.awslambda = l
        self.region = "us-west-2" if region is None else region
        self.s3 = s3
        self.codefile = codepath
        self.function_name = fname
        self.handler = handler
        self.role = os.environ.get('serverless_mapreduce_role')
        self.memory = lmem
        self.timeout = 900
        self.function_arn = None  # Lambda Function이 생성된 후에 설정됩니다.

    def create_lambda_function(self):
        '''
        AWS Lambda Function을 새로 생성하고 코드를 패키징한 zip 파일을 이용해 업데이트 합니다.
        '''
        runtime = 'python3.6'
        response = self.awslambda.create_function(
            FunctionName=self.function_name,
            Code={
                "ZipFile": open(self.codefile, 'rb').read()
            },
            Handler=self.handler,
            Role=self.role,
            Runtime=runtime,
            Description=self.function_name,
            MemorySize=self.memory,
            Timeout=self.timeout
        )
        self.function_arn = response['FunctionArn']
        print(response)

    def update_function(self):
        '''
        AWS Lambda Function의 코드를 패키징한 zip 파일을 이용해 업데이트 합니다.
        '''
        response = self.awslambda.update_function_code(
            FunctionName=self.function_name,
            ZipFile=open(self.codefile, 'rb').read(),
            Publish=True
        )
        updated_arn = response['FunctionArn']
        # parse arn and remove the release number (:n)
        arn = ":".join(updated_arn.split(':')[:-1])
        self.function_arn = arn
        print(response)

    def update_code_or_create_on_noexist(self):
        '''
        AWS Lambda Functions가 존재한다면 업데이트를 하고, 없다면 생성합니다.
        '''
        try:
            self.create_lambda_function()
        except botocore.exceptions.ClientError as e:
            # parse (Function already exist)
            self.update_function()

    def add_lambda_permission(self, sId, bucket):
        '''
        AWS Lambda의 권한(permission)을 설정합니다.
        S3 Bucket에서 이벤트시에 AWS Lambda를 Trigger 합니다.
        '''
        resp = self.awslambda.add_permission(
            Action='lambda:InvokeFunction',
            FunctionName=self.function_name,
            Principal='s3.amazonaws.com',
            StatementId='%s' % sId,
            SourceArn='arn:aws:s3:::' + bucket
        )
        print(resp)

    def create_s3_eventsource_notification(self, bucket, prefix=None):
        '''
        S3에서 발생하는 이벤트(Object 생성)를 Lambda function으로 알림(notifincation) 설정합니다.
        '''
        if not prefix:
            prefix = self.job_id + "/task/mapper"

        self.s3.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'Events': ['s3:ObjectCreated:*'],
                        'LambdaFunctionArn': self.function_arn,
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {
                                        'Name': 'prefix',
                                        'Value': prefix
                                    },
                                ]
                            }
                        }
                    }
                ],
                # 'TopicConfigurations' : [],
                # 'QueueConfigurations' : []
            }
        )

    def delete_function(self):
        '''
        등록된 AWS Lambda 함수(Function) 제거
        '''
        self.awslambda.delete_function(FunctionName=self.function_name)

    @classmethod
    def cleanup_logs(cls, func_name):
        '''
        CloudWatch에서 Lambda의 log group과 log streams을 제거합니다.
        '''
        log_client = boto3.client('logs')
        response = log_client.delete_log_group(logGroupName='/aws/lambda/' + func_name)
        return response
