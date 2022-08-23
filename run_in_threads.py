import queue
import threading

import json
import logging
import time

import redis

REDIS_HOST = common.read_environment_variable('REDIS_HOST', default='localhost')

redis_session = redis.Redis(host=REDIS_HOST, encoding="utf-8", decode_responses=True, socket_connect_timeout=1)

main_queue = queue.Queue()

running = False


def process_package_name(package_name):
    # if redis_session.exists(f'scs:only_tgz:{package_name}') or redis_session.exists(f'scs:not_only_tgz:{package_name}'):
    #     return
    # package_raw_data = package_utils.pypi_get_package_raw_data(package_name)
    # package_download_urls = package_utils.pypi_get_package_artifact_urls(package_raw_data)
    # if not package_download_urls:
    #     return
    # filtered_urls = filter(lambda x: x.endswith('gz'), package_download_urls)
    # filtered_urls = list(filtered_urls)
    # if len(filtered_urls) == len(package_download_urls):
    #     redis_session.set(f'scs:only_tgz:{package_name}', json.dumps(package_download_urls), ex=3600 * 72)
    # else:
    #     redis_session.set(f'scs:not_only_tgz:{package_name}', json.dumps(package_download_urls), ex=3600 * 72)
    print(package_name)


# def find_setup_py(package_name):
#     package_raw_data = package_utils.pypi_get_package_raw_data(package_name)
#     package_download_urls = package_utils.pypi_get_package_artifact_urls(package_raw_data)
#     if not package_download_urls:
#         return


def queue_wrapper(queue_insertion_func):
    for item in queue_insertion_func():
        main_queue.put(item)


def queue_wrapper_continuous(queue_insertion_func):
    while running:
        for item in queue_insertion_func():
            main_queue.put(item)


# def worker_wrapper(worker_func):
#     item = main_queue.get()
#     worker_func(item)

def worker_wrapper(worker_func):
    while running:
        item = main_queue.get()
        worker_func(item)


def run_in_threads(queue_function, worker_function, num_of_workers=6, queue_start_length=1000, continuous_insertion_to_queue=False):
    global running
    if continuous_insertion_to_queue:
        running = True
        queue_thread = threading.Thread(target=queue_wrapper_continuous, args=[queue_function])
    else:
        queue_thread = threading.Thread(target=queue_wrapper, args=[queue_function])
    queue_thread.start()
    while main_queue.qsize() < queue_start_length:
        logging.warning(f'queue is at {main_queue.qsize()} waiting for it to get to {queue_start_length}')
        time.sleep(3)

    threads = []
    for _ in range(num_of_workers):
        # threads.append(threading.Thread(target=worker_function, args=[main_queue.get()]))
        threads.append(threading.Thread(target=worker_wrapper, args=[worker_function]))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    current_queue_size = main_queue.qsize()
    if current_queue_size == 0:
        if not running:
            break
    logging.warning(f'{current_queue_size} left in queue')

    queue_thread.join()


run_in_threads(package_utils.pypi_get_all_package_names, process_package_name)
