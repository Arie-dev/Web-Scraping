from datetime import datetime
import io
import json
from bs4 import BeautifulSoup
from minio import Minio
import requests


def quotesBronzeLayer():
    minio_client = Minio('host.docker.internal:9000',
                         access_key='ROOTUSER',
                         secret_key='DATAINCUBATOR',
                         secure=False)
    # Create buckets if they don't exist
    for bucket in ['bronze', 'silver', 'gold']:
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            print(f"Bucket '{bucket}' created successfully")

    # Upload data to bronze bucket
    url = "http://quotes.toscrape.com/page/1/"
    response = requests.get(url)

    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        quote_blocks = soup.find_all('div', class_='quote')

        quotes_data = []
        for quote in quote_blocks:
            text = quote.find('span', class_='text').text.strip()
            author = quote.find('small', class_='author').text.strip()
            quotes_data.append({
                'text': text,
                'author': author,
            })

        current_date = datetime.now().strftime('%Y-%m-%d')
        object_name = f'luigi/quotes/quotes_data_{current_date}.json'

        # Convert books_data to JSON and upload to MinIO
        books_json = json.dumps(quotes_data, indent=4)
        # Convert to BytesIO for MinIO upload
        books_file = io.BytesIO(books_json.encode('utf-8'))

        minio_client.put_object(
            bucket_name='bronze',
            object_name=object_name,
            data=books_file,
            length=len(books_json),
            content_type='application/json'
        )
        print(f"Data successfully uploaded to 'bronze/{object_name}'")
        return quotes_data
    else:
        print(f"Failed to fetch page, status code: {response.status_code}")
        return None


if __name__ == "__main__":
    quotesBronzeLayer()
