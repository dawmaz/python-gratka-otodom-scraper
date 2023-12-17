import threading
import time

from gratka.gratka_cyclic_schedulers import consume_scheduled_jobs, consume_single_offer, consume_images, queue_defined_jobs


def defined_jobs():
    return threading.Thread(target=queue_defined_jobs, args=())


def process_defined_jobs():
    return threading.Thread(target=consume_scheduled_jobs, args=())


def process_single_offer():
    return threading.Thread(target=consume_single_offer, args=())


def process_images():
    return threading.Thread(target=consume_images, args=())


def thread_orchestrator(threads_dict):
    while True:
        print(f'Checking if threads are alive')
        for key, thread in threads_dict.items():
            if not thread.is_alive():
                if 'process_single_offer' in key:
                    new_thread = process_single_offer()
                elif 'process_images' in key:
                    new_thread = process_images()
                elif key == 'defined_jobs':
                    new_thread = defined_jobs()
                elif key == 'process_defined_jobs':
                    new_thread = process_defined_jobs()

                threads_dict[key] = new_thread
                new_thread.start()
                print(f'Thread {key} is dead. New one was created')

        time.sleep(30)



def run_extra_threads():
    threads = {}
    defined_jobs_thread = defined_jobs()
    threads['defined_jobs'] = defined_jobs_thread

    process_defined_jobs_thread = process_defined_jobs()
    threads['process_defined_jobs'] = process_defined_jobs_thread

    for i in range(8):
        x = process_single_offer()
        threads[f'process_single_offer_{i}'] = x
        y = process_images()
        threads[f'process_images_{i}'] = y

    for thread in threads.values():
        thread.start()

    orchestrator_thread = threading.Thread(target=thread_orchestrator, args=(threads,))
    orchestrator_thread.start()


if __name__ == '__main__':
    run_extra_threads()
