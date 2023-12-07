import datetime

from db_schema import create_session, LoggedError
from gratka_extractor import extract_links_from_url, extract_parameters
from offer_parser import offer_parse_parameters
from gratka_jobs import individual_offer_scan, photos_download, refresh_all, send_message_to_queue
from cyclic_schedulers import consume_scheduled_jobs, consume_single_offer, consume_images
import pika
import threading
from datetime import datetime


def main():
    links = prepare_links()
    all_to_check = []
    all_params = []
    unique_params = set()

    for link in links:
        all_to_check.extend(extract_links_from_url(link))

    for site in all_to_check:
        all_params.extend(extract_parameters(site))

    for param in all_params:
        unique_params.add(param[0])

    for param in unique_params:
        print(param)


def prepare_links():
    links = []
    initial_string = 'https://gratka.pl/nieruchomosci/mieszkania/wroclaw?page='
    for i in range(20):
        links.append(initial_string + str(i + 1))

    return links


def main2():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare a queue named 'hello'
    channel.queue_declare(queue='hello')

    channel.basic_publish(exchange='',
                          routing_key='hello',
                          body=b'Hello, RabbitMQ!')

    channel.basic_publish(exchange='',
                          routing_key='hello',
                          body=b'Drugie ')


def main3():
    # Establish a connection to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue='hello')

    # Set up the callback function to handle incoming messages
    channel.basic_consume(queue='hello',
                          on_message_callback=callback,
                          auto_ack=False)

    print(' [*] Waiting for messages. To exit, press CTRL+C')
    channel.basic_get(queue='hello')


def callback(ch, method, properties, body):
    # This function will be called when a message is received
    print(f" [x] Received {body.decode('utf-8')}")

    # Manually acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main4():
    params = extract_parameters('https://gratka.pl/nieruchomosci/mieszkanie-wroclaw-stare-miasto/ob/32607069')
    object = offer_parse_parameters(params)
    print()

def main5():
    individual_offer_scan("https://gratka.pl/nieruchomosci/mieszkanie-wroclaw-swojczyce-ul-magellana/ob/28965805")

def main6():
    photos_download('32607069,4,https://d-gr.cdngr.pl/kadry/k/r/gr-ogl/fd/65/32607069_871346193_mieszkanie-wroclaw-stare-miasto_xlarge.jpg')

def main7():
    refresh_all();

def mian8():
    send_message_to_queue('process_scheduled_jobs', ['refresh_all'])

def main9():
    consume_scheduled_jobs()


if __name__ == '__main__':
    try:
        raise Exception('This is sample exception')
    except Exception as e:
        with create_session() as session:
            l = LoggedError(process_name="Temp", value="value", date=datetime.now(), error_message=str(e))
            session.add(l)
    # threads = []
    # for i in range(20):
    #     thread = threading.Thread(target=consume_single_offer, args=())
    #     threads.append(thread)
    # for thread in threads:
    #     thread.start()


