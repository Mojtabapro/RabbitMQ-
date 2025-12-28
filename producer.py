import requests
import pika
import json

API_URL = "https://jsonplaceholder.typicode.com/posts"

response = requests.get(API_URL)
posts = response.json()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()


channel.queue_declare(queue='data_queue', durable=True)

for post in posts:
    channel.basic_publish(
        exchange='',
        routing_key='data_queue',
        body=json.dumps(post),
        properties=pika.BasicProperties(
            delivery_mode=2
        )
    )
    print(f"ارسال شد: Post ID={post['id']} \n")

connection.close()
print("✅ همه پیام‌ها ارسال شدند")
