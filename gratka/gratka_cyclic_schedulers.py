import threading
import logging
import time
from datetime import timedelta, datetime

import pika

from .gratka_db_schema import create_session, ScheduledJob, JobHistory, LoggedError
from .gratka_jobs import send_message_to_queue, full_scan, refresh_all, individual_offer_scan, photos_download

INTERVAL = 30
DAILY_REFRESH_PAGE_URL = 'https://gratka.pl/nieruchomosci/mieszkania/wroclaw?data-dodania-search=ostatnich-24h'
FULL_SCAN_PAGE_URL = 'https://gratka.pl/nieruchomosci/mieszkania/wroclaw'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__+'GRATKA_APP')


def consume_scheduled_jobs(connection_name):
    # Connection parameters
    connection_params = pika.ConnectionParameters(host='rabbit-mq', port=5672, client_properties={
        'connection_name': f'{connection_name}'})
    # Establish connection
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    # Declare the queue
    channel.queue_declare(queue='process_scheduled_jobs')
    # Set the prefetch count to limit the number of unacknowledged messages
    channel.basic_qos(prefetch_count=3)

    # Set up the callback function with manual acknowledgment
    channel.basic_consume(queue='process_scheduled_jobs', on_message_callback=process_scheduled_jobs_callback)

    channel.start_consuming()


def process_scheduled_jobs_callback(ch, method, properties, body):
    thread = threading.Thread(target=__process_scheduled_jobs_callback, args=(ch, method, body))
    thread.start()


def __process_scheduled_jobs_callback(ch, method, body):
    msg = body.decode('utf-8')
    logger.info(f'Received message : {msg}. Proper process will be started')

    if msg == 'daily_refresh':
        __process_job(msg, DAILY_REFRESH_PAGE_URL)
    elif msg == 'full_scan':
        __process_job(msg, FULL_SCAN_PAGE_URL)
    elif msg == 'refresh_all':
        __submit_job_history(msg, refresh_all())

    ch.basic_ack(delivery_tag=method.delivery_tag)


def __process_job(msg, page):
    processed_count = full_scan(page)
    logger.info(f'Job {msg} processed with count {processed_count}')
    __submit_job_history(msg, processed_count)


def __submit_job_history(msg, processed_count):
    with create_session() as session:
        job = session.query(ScheduledJob).filter_by(name=msg).first()
        job_history = JobHistory(job_id=job.id, rows_inserted=processed_count, run_date=datetime.now())
        job.last_run = datetime.now()
        session.add(job_history)
        session.merge(job)


def consume_single_offer(connection_name):
    # Connection parameters
    connection_params = pika.ConnectionParameters(host='rabbit-mq', port=5672, client_properties={
        'connection_name': f'{connection_name}'})
    # Establish connection
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    # Declare the queue
    channel.queue_declare(queue='process_single_offer')
    # Set the prefetch count to limit the number of unacknowledged messages
    channel.basic_qos(prefetch_count=10)

    # Set up the callback function with manual acknowledgment
    channel.basic_consume(queue='process_single_offer', on_message_callback=process_single_offer_callback)

    channel.start_consuming()


def process_single_offer_callback(ch, method, properties, body):
    msg = body.decode('utf-8')
    try:
        logger.info(f'Process single offer callback received with message: {msg}')
        individual_offer_scan(msg)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f'Individual offer scan failed for message {msg} with error: {e}')
        log_error_db('process_single_offer', msg, e)




def consume_images(connection_name):
    # Connection parameters
    connection_params = pika.ConnectionParameters(host='rabbit-mq', port=5672, client_properties={
        'connection_name': f'{connection_name}'})
    # Establish connection
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    # Declare the queue
    channel.queue_declare(queue='process_image')
    # Set the prefetch count to limit the number of unacknowledged messages
    channel.basic_qos(prefetch_count=10)

    # Set up the callback function with manual acknowledgment
    channel.basic_consume(queue='process_image', on_message_callback=process_images_callback)

    channel.start_consuming()


def process_images_callback(ch, method, properties, body):
    msg = body.decode('utf-8')
    try:
        logger.info(f'Process image callback received with message: {msg}')
        photos_download(msg)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        log_error_db('process_images', msg, e)
        logger.info(f'Image process failed for message {msg} with error: {e}')


def log_error_db(process_name, value, e):
    with create_session() as session:
        logged_error = LoggedError(process_name=process_name, value=value, date=datetime.now(), error_message=str(e))
        session.add(logged_error)


def queue_defined_jobs():
    while True:
        with create_session() as session:
            jobs = session.query(ScheduledJob).all()
            for job in jobs:
                if __should_process_job(job):
                    send_message_to_queue('process_scheduled_jobs', [job.name])
                    job.last_run = datetime.now()
                    session.merge(job)
        time.sleep(INTERVAL)


def __should_process_job(job):
    if job.last_run:
        frequency = job.frequency
        delta = get_time_delta(frequency)
        return __should_update(delta, job.last_run)
    else:
        return True


def get_time_delta(time_string):
    number = int(time_string[:-1])
    unit = time_string[-1]

    unit_mapping = {'D': 'days', 'H': 'hours'}

    if unit in unit_mapping:
        time_delta_args = {unit_mapping[unit]: number}
        return timedelta(**time_delta_args)


def __should_update(delta, date):
    current_time = datetime.now()
    time_difference = current_time - date
    return time_difference >= delta
