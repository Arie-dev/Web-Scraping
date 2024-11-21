from datetime import datetime
import io
import json
import re
import pandas as pd
from io import BytesIO
from minio import Minio

# Initialize MinIO client


def get_minio_client():
    return Minio('host.docker.internal:9000',
                 access_key='ROOTUSER',
                 secret_key='DATAINCUBATOR',
                 secure=False)

# Fetch the latest file from the bronze bucket


def get_latest_file(minio_client, bucket_name, prefix):
    objects = list(minio_client.list_objects(bucket_name, prefix=prefix))
    if objects:
        latest_object = max(objects, key=lambda obj: obj.last_modified)
        return latest_object.object_name
    else:
        raise FileNotFoundError(
            f"No files found in {bucket_name} with prefix {prefix}")

# Load data from the bronze bucket


def load_bronze_data(bucket_name, prefix):
    minio_client = get_minio_client()
    object_name = get_latest_file(minio_client, bucket_name, prefix)
    try:
        response = minio_client.get_object(bucket_name, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return json.loads(data.decode('utf-8'))
    except Exception as e:
        print(
            f"Error retrieving object {object_name} from bucket {bucket_name}: {e}")
        return None

# Clean books data


def clean_books_data(books_data):
    cleaned_data = []
    timestamp = datetime.now().isoformat()
    for book in books_data:
        price_str = re.sub(r'[^\d.]', '', book['price'])
        try:
            price = float(price_str)
        except ValueError:
            print(
                f"Could not convert price to float for book: {book['title']}")
            price = None
        availability = book['availability'].replace('\n', '').strip()
        enriched_book = {
            'title': book['title'],
            'price': price,
            'availability': availability,
            'rating': book['rating'],
            'scrape_timestamp': timestamp
        }
        cleaned_data.append(enriched_book)
    return cleaned_data

# Clean quotes data


def clean_quotes_data(quotes_data):
    cleaned_data = []
    timestamp = datetime.now().isoformat()
    for quote in quotes_data:
        if 'text' in quote and 'author' in quote:
            cleaned_quote = {
                'text': quote['text'],
                'author': quote['author'],
                'scrape_timestamp': timestamp
            }
            cleaned_data.append(cleaned_quote)
    return cleaned_data

# Save data to MinIO as a Parquet file


def save_data_to_minio_parquet(data, minio_client, bucket_name, object_name):
    df = pd.DataFrame(data)
    parquet_data = BytesIO()
    df.to_parquet(parquet_data, engine='pyarrow', index=False)
    parquet_data.seek(0)
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    minio_client.put_object(
        bucket_name, object_name, parquet_data, len(parquet_data.getvalue())
    )
    print(
        f"Data saved successfully as {object_name} in bucket '{bucket_name}'.")

# Process silver layer


def process_silver_layer():
    minio_client = get_minio_client()

    # Load data from bronze bucket
    books_data = load_bronze_data('bronze', 'luigi/books/')
    quotes_data = load_bronze_data('bronze', 'luigi/quotes/')

    if not books_data:
        print("No books data found in bronze layer.")
    if not quotes_data:
        print("No quotes data found in bronze layer.")

    # Clean data
    if books_data:
        cleaned_books_data = clean_books_data(books_data)
        save_data_to_minio_parquet(
            cleaned_books_data,
            minio_client,
            'silver',
            f'luigi/books_cleaned/books_data_silver_{datetime.now().strftime("%Y%m%d")}.parquet'
        )

    if quotes_data:
        cleaned_quotes_data = clean_quotes_data(quotes_data)
        save_data_to_minio_parquet(
            cleaned_quotes_data,
            minio_client,
            'silver',
            f'luigi/quotes_cleaned/quotes_data_silver_{datetime.now().strftime("%Y%m%d")}.parquet'
        )


if __name__ == "__main__":
    process_silver_layer()
