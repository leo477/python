import boto3

endpoint_url = 'http://localhost:4566'
bucket_name = 'raw-data'
localstack_access_key = 'test'
localstack_secret_key = 'test'

# Ініціалізація клієнта S3
s3_client = boto3.client(
    's3',
    endpoint_url=endpoint_url,
    aws_access_key_id=localstack_access_key,
    aws_secret_access_key=localstack_secret_key
)

s3_client.create_bucket(Bucket=bucket_name)
response = s3_client.list_objects_v2(Bucket=bucket_name)

print("Список файлів в бакеті:")
for obj in response.get('Contents', []):
    print(obj['Key'])
local_file_path = 'homework9.py'
s3_object_key = 'homework9.py'
s3_client.upload_file(local_file_path, bucket_name, s3_object_key)

print(f"Файл {s3_object_key} завантажено в бакет {bucket_name}")
