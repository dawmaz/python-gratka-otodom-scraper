import os
import threading
from datetime import datetime, timedelta

import requests
import base64
import json

from gratka.gratka_db_schema import create_session
from gratka.gratka_jobs import send_message_to_queue
from otodom.otodom_db_schema import OtodomOffer, OtodomPhotos, OtodomPriceHistory
from otodom.otodom_extractor import extract_page_number, prepare_links, extract_links_from_url, extract_parameters
from otodom.otodom_offer_parser import offer_parse_parameters


class SharedData:
    lock = threading.Lock()


def full_scan(url):
    page_numbers = extract_page_number(url)
    links = prepare_links(url, page_numbers)
    all_to_check = []

    for link in links:
        all_to_check.extend(extract_links_from_url(link))

    send_message_to_queue('otodom_process_single_offer', all_to_check)
    return len(all_to_check)


def refresh_all():
    one_day_ago = datetime.now() - timedelta(days=1)
    with create_session() as session:
        offers = session.query(OtodomOffer).filter(OtodomOffer.date_removed.is_(None),
                                                   OtodomOffer.last_visited <= one_day_ago).all()
        offers_addresses = [offer.website_address for offer in offers]
        send_message_to_queue('otodom_process_single_offer', offers_addresses)
        return len(offers)


def individual_offer_scan(url):
    with create_session() as session:
        existing_offer = session.query(OtodomOffer).filter_by(website_address=url).first()
        # Do not process again if the site was visited in last 24h

        if existing_offer:
            current_time = datetime.now()
            time_difference = current_time - existing_offer.last_visited
            if time_difference >= timedelta(hours=17):
                url_params = extract_parameters(url)
                is_removed = url_params[0] == 'Removed'
                if is_removed:
                    existing_offer.date_removed = current_time
                    session.merge(existing_offer)
                    return

                new_offer = offer_parse_parameters(url_params)
                new_offer.website_address = url
                update_dates(new_offer, existing_offer)
                if new_offer.price != existing_offer.price:
                    update_prices(new_offer, existing_offer)
                    create_price_history(existing_offer)
                session.merge(existing_offer)
        else:
            url_params = extract_parameters(url)
            is_removed = url_params[0] == 'Removed'
            if not is_removed:
                new_offer = offer_parse_parameters(url_params)
                new_offer.website_address = url
                new_offer.offer_id = url.split('/')[-1]
                update_dates(new_offer, existing_offer)
                create_price_history(new_offer)
                session.add(new_offer)
                session.commit()
                add_download_photos_to_queue(url_params, new_offer.offer_id, new_offer.id)


def photos_download(offerid_link):
    offer_id, foreign_id, image_url = offerid_link.split(',')
    offer_id = 'images_otodom/' + offer_id
    response = requests.get(image_url)

    parts = image_url.split("/")

    # Find the part that starts with "eyJmbiI"
    id_part = next(part for part in parts if part.startswith("ey")).split('.')[0]

    id_part += '=' * ((4 - len(id_part) % 4) % 4)

    decoded_token = base64.urlsafe_b64decode(id_part).decode('utf-8')

    json_object = json.loads(decoded_token)

    # Extract the value for the "fn" key
    id_value = json_object.get("fn")

    if response.status_code == 200:
        with SharedData.lock:
            if not os.path.exists(offer_id):
                os.makedirs(offer_id)

        file_name = f'{id_value}.jpg'
        destination = os.path.join(offer_id, file_name)
        with open(destination, 'wb') as file:
            file.write(response.content)

        with create_session() as session:
            photo = OtodomPhotos(otodom_offer_id=int(foreign_id), path=destination, original_web_address=image_url)
            session.add(photo)


def add_download_photos_to_queue(params, offer_id, foreign_id):
    images = [data for data in params if data[0] == 'images'][0][1]
    offer_id = offer_id.split('-')[-1]
    concat_images_with_id = [f'{offer_id},{foreign_id},{img}' for img in images]
    send_message_to_queue('otodom_process_image', concat_images_with_id)


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
    price_history = OtodomPriceHistory(price=offer.price, date=offer.last_visited)
    offer.otodom_price_history.append(price_history)
