from datetime import datetime
import pandas as pd
from io import BytesIO, StringIO
from minio import Minio
from textblob import TextBlob

# Initialize MinIO client


def get_minio_client():
    return Minio('host.docker.internal:9000',
                 access_key='ROOTUSER',
                 secret_key='DATAINCUBATOR',
                 secure=False)

# Fetch the latest Parquet file from the silver bucket


def get_latest_parquet_file(minio_client, bucket_name, prefix):
    objects = list(minio_client.list_objects(bucket_name, prefix=prefix))
    if objects:
        latest_object = max(objects, key=lambda obj: obj.last_modified)
        return latest_object.object_name
    else:
        raise FileNotFoundError(
            f"No files found in {bucket_name} with prefix {prefix}")

# Load Parquet data from MinIO


def load_parquet_from_minio(bucket_name, prefix):
    minio_client = get_minio_client()
    object_name = get_latest_parquet_file(minio_client, bucket_name, prefix)
    try:
        response = minio_client.get_object(bucket_name, object_name)
        data = pd.read_parquet(BytesIO(response.read()))
        response.close()
        response.release_conn()
        print(
            f"Loaded Parquet file '{object_name}' from bucket '{bucket_name}'.")
        return data
    except Exception as e:
        print(
            f"Error retrieving Parquet file from bucket '{bucket_name}': {e}")
        return None

# Add price category to books data


def add_price_category(books_data):
    for book in books_data:
        if book['price'] < 10:
            book['price_category'] = 'cheap'
        elif book['price'] < 20:
            book['price_category'] = 'moderate'
        else:
            book['price_category'] = 'expensive'
    return books_data

# Add sentiment analysis to quotes data


def add_sentiment_analysis(quotes_data):
    enriched_data = []
    for quote in quotes_data:
        sentiment = TextBlob(quote['text']).sentiment.polarity
        enriched_quote = quote.copy()
        enriched_quote['sentiment'] = sentiment
        enriched_data.append(enriched_quote)
    return enriched_data

# Save data to MinIO as a CSV file


def save_data_to_minio_csv(data, minio_client, bucket_name, object_name):
    df = pd.DataFrame(data)
    csv_data = StringIO()
    df.to_csv(csv_data, index=False)
    csv_data.seek(0)
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    minio_client.put_object(
        bucket_name, object_name, BytesIO(
            csv_data.getvalue().encode('utf-8')), len(csv_data.getvalue())
    )
    print(
        f"Data saved successfully as {object_name} in bucket '{bucket_name}'.")

# Process gold layer


def process_gold_layer():
    minio_client = get_minio_client()

    # Load books and quotes data from the silver bucket
    books_df = load_parquet_from_minio('silver', 'luigi/books_cleaned/')
    quotes_df = load_parquet_from_minio('silver', 'luigi/quotes_cleaned/')

    # Perform gold-level transformations
    if books_df is not None:
        gold_books_data = add_price_category(
            books_df.to_dict(orient='records'))
        save_data_to_minio_csv(
            gold_books_data,
            minio_client,
            'gold',
            f'luigi/books/books_data_gold_{datetime.now().strftime("%Y%m%d")}.csv'
        )

    if quotes_df is not None:
        gold_quotes_data = add_sentiment_analysis(
            quotes_df.to_dict(orient='records'))
        save_data_to_minio_csv(
            gold_quotes_data,
            minio_client,
            'gold',
            f'luigi/quotes/quotes_data_gold_{datetime.now().strftime("%Y%m%d")}.csv'
        )


if __name__ == "__main__":
    process_gold_layer()
