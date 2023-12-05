import pika

from gratka_extractor import extract_page_number, prepare_links, extract_parameters
from offer_parser import offer_parse_parameters
from db_schema import create_session, Offer, JobHistory, PriceHistory
from datetime import datetime, timedelta


def full_scan(url):
    page_numbers = extract_page_number(url)
    links = prepare_links(url, page_numbers)

    send_message_to_kafka_queue('process_single_offer', links)


def individual_offer_scan(url):
    with create_session() as session:
        existing_offer = session.query(Offer).filter_by(website_address=url).first()
        url_params = extract_parameters(url)

        # Do not process again if the site was visited in last 24h
        if existing_offer:
            current_time = datetime.now()
            time_difference = current_time - existing_offer.last_visited
            if time_difference >= timedelta(minutes=1):
                new_offer = offer_parse_parameters(url_params)
                new_offer.website_address = url
                new_offer.offer_id = url.split('/')[-1]
                update_dates(new_offer, existing_offer)
                if new_offer.price != existing_offer.price:
                    update_prices(new_offer, existing_offer)
                    create_price_history(existing_offer)
                session.merge(existing_offer)
        else:
            new_offer = offer_parse_parameters(url_params)
            new_offer.website_address = url
            new_offer.offer_id = url.split('/')[-1]
            update_dates(new_offer, existing_offer)
            create_price_history(new_offer)
            session.add(new_offer)
            add_download_photos_to_queue(url_params, new_offer.offer_id)

def add_download_photos_to_queue(params, offer_id):
    images = [data for data in params if data[0] == 'image_list'][0][1]
    concat_images_with_id = [f'{offer_id},{img}'for img in images]
    send_message_to_kafka_queue('process_image', concat_images_with_id)


def update_dates(new_offer, existing_offer):
    current_time = datetime.now()
    if not existing_offer:
        new_offer.last_visited = current_time
        new_offer.date_added = current_time
    else:
        existing_offer.last_visited = current_time


def update_prices(new_offer, existing_offer):
    existing_offer.price = new_offer.price
    existing_offer.price_per_square_meter = new_offer.price_per_square_meter


def create_price_history(offer):
    price_history = PriceHistory(price=offer.price, date=offer.last_visited)
    offer.price_history.append(price_history)

def send_message_to_kafka_queue(queue_name, elements):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name)

    for element in elements:
        encoded_element = element.encode('utf-8')
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=encoded_element)