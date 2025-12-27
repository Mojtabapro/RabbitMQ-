import pika
import json
import time


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

print("در انتظار دریافت پیام...")

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f"دریافت شد: {message}")


    time.sleep(2)
    print(f"پردازش انجام شد برای ID={message['id']}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='task_queue',
    on_message_callback=callback
)

channel.start_consuming()
