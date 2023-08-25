import psycopg2
import boto3
import pandas as pd
from io import BytesIO

def get_postgres_connection():
    host = input("PostgreSQL Host: ")
    port = input("Port: ")
    dbname = input("Database Name: ")
    user = input("User: ")
    password = input("Password: ")
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    return conn

def get_s3_client():
    access_key = input("AWS Access Key: ")
    secret_key = input("AWS Secret Key: ")
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3 = session.client('s3')
    return s3

def list_folders(s3_client, bucket_name, prefix=''):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    common_prefixes = response.get('CommonPrefixes', [])
    return [common_prefix['Prefix'] for common_prefix in common_prefixes]

def main():
    conn = get_postgres_connection()
    s3_client = get_s3_client()
    
    while True:
        schema_name = input("Enter the Schema Name: ")
        table_name = input("Enter the Table Name: ")
        bucket_name = input("Enter the S3 Bucket Name: ")
        folders = list_folders(s3_client, bucket_name)
        
        if len(folders) == 0:
            print("No subfolders found in the bucket. Files will be saved to the root of the bucket.")
            selected_folder = ""
        else:
            selected_folder = choose_from_list(folders, "Select a folder for saving:")
        
        format_choice = input("Choose the file format (CSV or Parquet): ").lower()
        
        if format_choice == 'csv':
            file_extension = 'csv'
        elif format_choice == 'parquet':
            file_extension = 'parquet'
        else:
            print("Invalid format choice. Defaulting to CSV.")
            file_extension = 'csv'
        
        object_key = input("Enter the Object Key (file name) to save: ")
        s3_path = f"{selected_folder}{object_key}.{file_extension}"
        
        query = f"SELECT * FROM {schema_name}.{table_name}"
        df = pd.read_sql(query, conn)
        
        # Create a buffer to store the data
        buffer = BytesIO()
        
        if format_choice == 'csv':
            df.to_csv(buffer, index=False)
        elif format_choice == 'parquet':
            df.to_parquet(buffer, index=False)
        
        # Upload the data to S3
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, bucket_name, s3_path)
        
        print(f"File saved successfully to s3://{bucket_name}/{s3_path}.")
        
        continue_choice = input("Do you want to continue? (yes/no): ")
        if continue_choice.lower() != 'yes':
            break

if __name__ == "__main__":
    main()
