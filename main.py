import threading
import time
import requests

from gratka.gratka_cyclic_schedulers import consume_scheduled_jobs, consume_single_offer, consume_images, \
    queue_defined_jobs, log_error_db
from otodom import otodom_cyclic_schedulers
from otodom.otodom_extractor import extract_parameters
from otodom.otodom_jobs import individual_offer_scan


def defined_jobs():
    return threading.Thread(target=queue_defined_jobs, args=())


def process_defined_jobs(connection_name):
    return threading.Thread(target=consume_scheduled_jobs, args=(connection_name,))


def process_single_offer(connection_name):
    return threading.Thread(target=consume_single_offer, args=(connection_name,))


def process_images(connection_name):
    return threading.Thread(target=consume_images, args=(connection_name,))


def defined_jobs_otodom():
    return threading.Thread(target=otodom_cyclic_schedulers.queue_defined_jobs, args=())


def process_defined_jobs_otodom(connection_name):
    return threading.Thread(target=otodom_cyclic_schedulers.consume_scheduled_jobs, args=(connection_name,))


def process_single_offer_otodom(connection_name):
    return threading.Thread(target=otodom_cyclic_schedulers.consume_single_offer, args=(connection_name,))


def process_images_otodom(connection_name):
    return threading.Thread(target=otodom_cyclic_schedulers.consume_images, args=(connection_name,))


def thread_orchestrator(threads_dict):
    required_active_connections = ['process_defined_jobs',
                                   'process_defined_jobs_otodom',
                                   'process_single_offer_0',
                                   'process_single_offer_1',
                                   'process_single_offer_2',
                                   'process_single_offer_3',
                                   'process_single_offer_4',
                                   'process_single_offer_5',
                                   'process_single_offer_6',
                                   'process_single_offer_7',
                                   'process_single_offer_otodom_0',
                                   'process_single_offer_otodom_1',
                                   'process_single_offer_otodom_2',
                                   'process_single_offer_otodom_3',
                                   'process_single_offer_otodom_4',
                                   'process_single_offer_otodom_5',
                                   'process_single_offer_otodom_6',
                                   'process_single_offer_otodom_7',
                                   'process_images_0',
                                   'process_images_1',
                                   'process_images_2',
                                   'process_images_3',
                                   'process_images_4',
                                   'process_images_5',
                                   'process_images_6',
                                   'process_images_7',
                                   'process_images_otodom_0',
                                   'process_images_otodom_1',
                                   'process_images_otodom_2',
                                   'process_images_otodom_3',
                                   'process_images_otodom_4',
                                   'process_images_otodom_5',
                                   'process_images_otodom_6',
                                   'process_images_otodom_7',
                                   ]
    while True:
        try:
            time.sleep(30)
            connections = get_alive_connections()
            connections_to_reborn = get_dead_connections(connections, required_active_connections)

            if len(connections_to_reborn) > 0:
                reborn_threads(threads_dict, connections_to_reborn)

            active_threads = [thread for thread in threading.enumerate() if not thread.daemon]

            print(f'Active non deamon threads: {len(active_threads)}')
        except Exception as e:
            log_error_db('main_thread', 'exception', e)


def get_alive_connections():
    api_url = f'http://localhost:15672/api/connections'
    response = requests.get(api_url, auth=('guest', 'guest'))

    if response.status_code == 200:
        connections = response.json()
        return connections
    else:
        print(f"Failed to retrieve consumers. Status code: {response.status_code}")
        return None


def get_dead_connections(consumer, consumer_names_to_check):
    return [name for name in consumer_names_to_check if
            all(name != c['client_properties']['connection_name'] for c in consumer)]


def reborn_threads(threads_dict, to_reborn):
    for name in to_reborn:
        new_thread = None
        if 'process_defined_jobs' in name:
            if 'process_defined_jobs' == name:
                new_thread = process_defined_jobs(name)
            else:
                new_thread = process_defined_jobs_otodom(name)
        elif 'process_single_offer' in name:
            if 'otodom' not in name:
                new_thread = process_single_offer(name)
            else:
                new_thread = process_single_offer_otodom(name)
        elif 'process_images' in name:
            if 'otodom' not in name:
                new_thread = process_images(name)
            else:
                new_thread = process_images_otodom(name)

        old_thread = threads_dict[name]
        if old_thread and old_thread.is_alive():
            old_thread.join(1)
        threads_dict[name] = new_thread
        new_thread.start()
        print(f'Thread {name} is dead. New one was reborn')


def run_extra_threads():
    threads = {'defined_jobs': defined_jobs(),
               'defined_jobs_otodom': defined_jobs_otodom(),
               'process_defined_jobs': process_defined_jobs('process_defined_jobs'),
               'process_defined_jobs_otodom': process_defined_jobs_otodom('process_defined_jobs_otodom')}

    for i in range(8):
        threads[f'process_single_offer_{i}'] = process_single_offer(f'process_single_offer_{i}')
        threads[f'process_single_offer_otodom_{i}'] = process_single_offer_otodom(f'process_single_offer_otodom_{i}')
        threads[f'process_images_{i}'] = process_images(f'process_images_{i}')
        threads[f'process_images_otodom_{i}'] = process_images_otodom(f'process_images_otodom_{i}')

    for thread in threads.values():
        thread.start()

    orchestrator_thread = threading.Thread(target=thread_orchestrator, args=(threads,))
    orchestrator_thread.start()


if __name__ == '__main__':
    run_extra_threads()
