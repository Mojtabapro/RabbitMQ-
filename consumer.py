import pika
import json
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

channel.queue_declare(queue='data_queue', durable=True)

print("⏳ در انتظار دریافت داده‌ها...")

def callback(ch, method, properties, body):
    post = json.loads(body)

    print("📥 پست دریافت شد")
    print(f"ID: {post['id']}")
    print(f"عنوان: {post['title']}")

    time.sleep(1)

    print("✅ پردازش انجام شد\n")

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue='data_queue',
    on_message_callback=callback
)

channel.start_consuming()
