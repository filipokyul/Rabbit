import pika
import datetime
from sys import argv
import random

def create_message(param: str):
    now = datetime.datetime.now()
    time_part = str(now.minute) + '|'
    if param == "1":
        return time_part + str(random.randint(1, 19))
    elif param == "2":
        return time_part + str(random.randint(20, 39))
    elif param == "3":
        return time_part + str(random.randint(40, 59))
    else:
        raise Exception("This API is absent")

def main(param: str):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='w')
    message = create_message(param)
    channel.basic_publish(exchange='', routing_key='w', body=message)
    print(message)
    connection.close()

if __name__ == "__main__":
    main(argv[1])
