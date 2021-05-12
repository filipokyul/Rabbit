import pika
import redis

def callback(ch, method, properties, body):
    data = body.decode().split('|')
    weather_key = data[0]
    weather_value = data[1]
    r = redis.StrictRedis(host='localhost', port=6379, db=1)
    r.rpush(weather_key, weather_value)
    print([weather_key, weather_value])

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='w')
    channel.basic_consume(queue='w', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    #channel.stop_consuming()

if __name__ == '__main__':
    main()