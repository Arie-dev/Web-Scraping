import streamlit as st
import pandas as pd
from minio import Minio
from io import BytesIO


def setup_minio_client():
    minio_client = Minio('localhost:9000',
                         access_key='ROOTUSER',
                         secret_key='DATAINCUBATOR',
                         secure=False)

    if not minio_client.bucket_exists('gold'):
        st.error("Bucket 'gold' does not exist. Please check your setup.")
    return minio_client

def list_files_in_bucket(minio_client, bucket_name):
    try:
        objects = minio_client.list_objects(bucket_name)
        file_list = [obj.object_name for obj in objects]
        return file_list
    except Exception as e:
        st.error(f"Error listing files in bucket: {e}")
        return []


def get_csv_from_minio(minio_client, bucket_name, object_name):
    try:
        response = minio_client.get_object(bucket_name, object_name)
        data = BytesIO(response.read())
        return pd.read_csv(data) 
    except Exception as e:
        st.error(f"Error fetching CSV from MinIO: {e}")
        return None


def main():
    st.title("View CSV Data from MinIO")


    minio_client = setup_minio_client()

    
    bucket_name = "gold"
    files = list_files_in_bucket(minio_client, bucket_name)

    if not files:
        st.error("No files found in the 'gold' bucket.")
        return

    
    selected_file = st.selectbox("Select a CSV file to view:", files)

    if st.button("Load Data"):
        df = get_csv_from_minio(minio_client, bucket_name, selected_file)
        if df is not None:
            st.write("CSV Data:")
            st.dataframe(df)
            st.write("Summary:")
            st.write(df.describe())

if __name__ == "__main__":
    main()
