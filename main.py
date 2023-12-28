import threading
import time
import requests

from gratka.gratka_cyclic_schedulers import consume_scheduled_jobs, consume_single_offer, consume_images, \
    queue_defined_jobs
from otodom import otodom_cyclic_schedulers


def defined_jobs():
    return threading.Thread(target=queue_defined_jobs, args=())


def process_defined_jobs():
    return threading.Thread(target=consume_scheduled_jobs, args=())


def process_single_offer():
    return threading.Thread(target=consume_single_offer, args=())


def process_images():
    return threading.Thread(target=consume_images, args=())


def defined_jobs_otodom():
    return threading.Thread(target=otodom_cyclic_schedulers.queue_defined_jobs, args=())


def process_defined_jobs_otodom():
    return threading.Thread(target=otodom_cyclic_schedulers.consume_scheduled_jobs, args=())


def process_single_offer_otodom():
    return threading.Thread(target=otodom_cyclic_schedulers.consume_single_offer, args=())


def process_images_otodom():
    return threading.Thread(target=otodom_cyclic_schedulers.consume_images, args=())


def thread_orchestrator(threads_dict):
    while True:
        print(f'Checking if threads are alive')
        for key, thread in threads_dict.items():
            if not thread.is_alive():
                if 'process_single_offer' in key:
                    if 'otodom' in key:
                        new_thread = process_single_offer_otodom()
                    else:
                        new_thread = process_single_offer()
                elif 'process_images' in key:
                    if 'otodom' in key:
                        new_thread = process_images_otodom()
                    else:
                        new_thread = process_single_offer()
                elif 'process_images' in key:
                    if 'otodom' in key:
                        new_thread = process_images_otodom()
                    else:
                        new_thread = process_images()
                elif key == 'defined_jobs':
                    if 'otodom' in key:
                        new_thread = defined_jobs_otodom()
                    else:
                        new_thread = defined_jobs()
                elif key == 'process_defined_jobs':
                    if 'otodom' in key:
                        new_thread = process_defined_jobs_otodom()
                    else:
                        new_thread = process_defined_jobs()

                threads_dict[key] = new_thread
                new_thread.start()
                print(f'Thread {key} is dead. New one was created')

        consumers = None#get_alive_consumers()

        if consumers:
            to_reborn = check_consumers_alive(consumers, 'otodom_process_scheduled_jobs', 'process_scheduled_jobs')
            reborn_threads(threads_dict, to_reborn)

        time.sleep(30)


def get_alive_consumers():
    api_url = f'http://localhost:15672/api/consumers'
    response = requests.get(api_url, auth=('guest', 'guest'))

    if response.status_code == 200:
        consumers = response.json()
        return consumers
    else:
        print(f"Failed to retrieve consumers. Status code: {response.status_code}")
        return None


def check_consumers_alive(consumer, *consumer_names_to_check):
    return [name for name in consumer_names_to_check if all(name != c['queue']['name'] for c in consumer)]


def reborn_threads(threads_dict, to_reborn):
    if to_reborn:
        for name in to_reborn:
            new_thread = None
            if 'otodom_process_scheduled_jobs' in name:
                new_thread = process_defined_jobs_otodom()
                old_thread = threads_dict['process_defined_jobs_otodom']
                if old_thread and old_thread.is_alive():
                    old_thread.join(1)
                threads_dict['process_defined_jobs_otodom'] = new_thread
            elif 'process_scheduled_jobs' in name:
                new_thread = process_defined_jobs()
                old_thread = threads_dict['process_defined_jobs']
                if old_thread and old_thread.is_alive():
                    old_thread.join(1)
                threads_dict['process_defined_jobs'] = new_thread

            new_thread.start()
            print(f'Thread {name} is dead. New one was reborn')


def run_extra_threads():
    threads = {}

    threads['defined_jobs'] = defined_jobs()
    threads['defined_jobs_otodom'] = defined_jobs_otodom()

    threads['process_defined_jobs'] = process_defined_jobs()
    threads['process_defined_jobs_otodom'] = process_defined_jobs_otodom()

    for i in range(8):
        threads[f'process_single_offer_{i}'] = process_single_offer()
        threads[f'process_single_offer_otodom_{i}'] = process_single_offer_otodom()
        threads[f'process_images_{i}'] = process_images()
        threads[f'process_images_otodom_{i}'] = process_images_otodom()

    for thread in threads.values():
        thread.start()

    orchestrator_thread = threading.Thread(target=thread_orchestrator, args=(threads,))
    orchestrator_thread.start()


if __name__ == '__main__':
    run_extra_threads()


