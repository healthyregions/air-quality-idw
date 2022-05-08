# %%
import boto3
import os
from datetime import date

today = date.today().strftime("%b-%d-%Y")

AWS_ACCESS_KEY_ID = os.getenv('S3_DEPLOYER_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('S3_DEPLOYER_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET')

dir_path = os.path.dirname(os.path.realpath(__file__))
repo_root = os.path.abspath(os.path.join(dir_path, '..'))
data_dir = os.path.join(repo_root, 'temp')

files_to_upload = [
    'data_summary.csv',
    'raw_data.csv',
    'interpolated_data.csv',
]
# %%

def write_to_s3(filename, s3, bucket, subfolder, pathToFolder, prefix=""):
    try:
        print(f'Writing {filename} to S3...')
        s3.meta.client.upload_file(os.path.join(pathToFolder, filename), bucket, f'{subfolder}/{prefix}_{filename}')
        print('Write to S3 complete.')

    except Exception as e:
        print(e)
#%%
if __name__ == '__main__':

    client = boto3.resource('s3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = client.Bucket(BUCKET_NAME)

    for file in files_to_upload:
        write_to_s3(file, s3, BUCKET_NAME, 'data', data_dir)
        write_to_s3(file, s3, BUCKET_NAME, 'archive', data_dir, prefix=today)

    print('Upload complete.')