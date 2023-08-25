import boto3
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO

def main():
    # Prompt user for AWS credentials
    aws_access_key = input("Enter your AWS Access Key: ")
    aws_secret_key = input("Enter your AWS Secret Key: ")

    # Set up S3 connection
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # Prompt user for PostgreSQL credentials
    db_host = input("Enter your PostgreSQL host: ")
    db_port = input("Enter your PostgreSQL port: ")
    db_name = input("Enter your PostgreSQL database name: ")
    db_user = input("Enter your PostgreSQL user: ")
    db_password = input("Enter your PostgreSQL password: ")

    # Set up PostgreSQL connection
    engine = create_engine(
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    # Prompt user for S3 bucket and file path
    bucket_name = input("Enter your S3 bucket name: ")
    file_name = None

    while file_name is None or (not file_name.endswith(('.parquet', '.csv'))):
        file_name = input("Path of the Parquet or CSV file in the bucket (e.g., 'folder/file.parquet' or file on root use file.parquet):")
        if not file_name.endswith(('.parquet', '.csv')):
            print("Unsupported file format. Please use a Parquet or CSV file.")

    # Read Parquet or CSV file from S3 bucket
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    buffer = BytesIO(obj['Body'].read())

    if file_name.endswith('.parquet'):
        df = pd.read_parquet(buffer)
    else:
        df = pd.read_csv(buffer)

    # Insert data into PostgreSQL table
    schema_name = input("Enter the schema name in PostgreSQL: ")
    table_name = input("Enter the table name in PostgreSQL: ")
    df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
    
    print("Data saved successfully!")

if __name__ == "__main__":
    main()

