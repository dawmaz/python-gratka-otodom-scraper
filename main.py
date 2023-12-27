import threading
import time

from gratka.gratka_cyclic_schedulers import consume_scheduled_jobs, consume_single_offer, consume_images, queue_defined_jobs
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

        time.sleep(30)


def run_extra_threads():
    threads = {}
    defined_jobs_thread = defined_jobs()
    defined_jobs_thread_otodom = defined_jobs_otodom()

    threads['defined_jobs'] = defined_jobs_thread
    threads['defined_jobs_otodom'] = defined_jobs_thread_otodom

    process_defined_jobs_thread = process_defined_jobs()
    process_defined_jobs_thread_otodom = process_defined_jobs_otodom()

    threads['process_defined_jobs'] = process_defined_jobs_thread
    threads['process_defined_jobs_otodom'] = process_defined_jobs_thread_otodom

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
