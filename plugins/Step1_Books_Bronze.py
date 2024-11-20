from bs4 import BeautifulSoup
from minio import Minio
import requests
import json
import io
from datetime import datetime


def booksBronzeLayer():
    minio_client = Minio('host.docker.internal:9000',
                         access_key='ROOTUSER',
                         secret_key='DATAINCUBATOR',
                         secure=False)
    # Create buckets if they don't exist
    for bucket in ['bronze', 'silver', 'gold']:
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            print(f"Bucket '{bucket}' created successfully")

    # Fetch data from the website
    url = "https://books.toscrape.com/catalogue/page-1.html"
    response = requests.get(url)

    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        book_rows = soup.find_all('article', class_='product_pod')

        books_data = []
        for book in book_rows:
            title = book.find('h3').find('a')['title']
            price = book.find('p', class_='price_color').text.strip()
            availability = book.find(
                'p', class_='instock availability').text.strip()
            rating = book.find('p', class_='star-rating')['class'][1]

            books_data.append({
                'title': title,
                'price': price,
                'availability': availability,
                'rating': rating
            })

        current_date = datetime.now().strftime('%Y-%m-%d')
        object_name = f'books/books_data_{current_date}.json'

        # Convert books_data to JSON and upload to MinIO
        books_json = json.dumps(books_data, indent=4)
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
        return books_data
    else:
        print(f"Failed to fetch page, status code: {response.status_code}")
        return None


if __name__ == "__main__":
    booksBronzeLayer()
