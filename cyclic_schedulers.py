import time
import pika

from db_schema import create_session, ScheduledJob, JobHistory
from datetime import timedelta, datetime
from gratka_jobs import send_message_to_queue, full_scan, refresh_all, individual_offer_scan, photos_download

INTERVAL = 30
DAILY_REFRESH_PAGE_URL = 'https://gratka.pl/nieruchomosci/mieszkania/wroclaw?data-dodania-search=ostatnich-24h'
FULL_SCAN_PAGE_URL = 'https://gratka.pl/nieruchomosci/mieszkania/wroclaw'


def process_scheduled_jobs_callback(ch, method, properties, body):
    msg = body.decode('utf-8')

    if msg == 'daily_refresh':
        __process_job(msg, DAILY_REFRESH_PAGE_URL)
    elif msg == 'full_scan':
        __process_job(msg, FULL_SCAN_PAGE_URL)
    elif msg == 'refresh_all':
        __submit_job_history(msg, refresh_all())

    # Manually acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


def __process_job(msg, page):
    processed_count = full_scan(page)
    __submit_job_history(msg, processed_count)


def __submit_job_history(msg, processed_count):
    with create_session() as session:
        job = session.query(ScheduledJob).filter_by(name=msg).first()
        job_history = JobHistory(job_id=job.id, rows_inserted=processed_count, run_date=datetime.now())
        job.last_run = datetime.now()
        session.add(job_history)
        session.merge(job)


def process_single_offer_callback(ch, method, properties, body):
    msg = body.decode('utf-8')
    individual_offer_scan(msg)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def process_images_callback(ch, method, properties, body):
    msg = body.decode('utf-8')
    photos_download(msg)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_scheduled_jobs():
    # Connection parameters
    connection_params = pika.ConnectionParameters(host='localhost', port=5672)
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


def consume_single_offer():
    # Connection parameters
    connection_params = pika.ConnectionParameters(host='localhost', port=5672)
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


def consume_images():
    # Connection parameters
    connection_params = pika.ConnectionParameters(host='localhost', port=5672)
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


def queue_defined_jobs():
    while True:
        with create_session() as session:
            jobs = session.query(ScheduledJob).all()
            for job in jobs:
                if __should_process_job(job):
                    send_message_to_queue('process_scheduled_jobs', [job.name])
                    pass
            time.sleep(INTERVAL)


def __should_process_job(job):
    if job.last_run:
        frequency = job.frequency
        delta = get_time_delta(frequency)
        return __should_update(delta, job.last_run)
    else:
        return True


def get_time_delta(frequency):
    if frequency == '1D':
        return timedelta(days=1)
    elif frequency == '2D':
        return timedelta(days=2)


def __should_update(delta, date):
    current_time = datetime.now()
    time_difference = current_time - date
    return time_difference >= delta