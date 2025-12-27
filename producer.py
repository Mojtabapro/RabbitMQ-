import pika
import json
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

for i in range(5):
    message = {
        "id": i,
        "task": "process_data",
        "value": i * 10
    }

    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2  # ذخیره پیام روی دیسک (Persistent)
        )
    )

    print(f"ارسال شد: {message}")
    time.sleep(1)

connection.close()
