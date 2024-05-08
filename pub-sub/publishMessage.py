
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
import json
import sys

def publishMessage(project_id: str, topic_id: str, data: str) -> pubsub_v1.publisher.futures.Future:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    publish_future = publisher.publish(topic_path, data)
    publish_future.add_done_callback(publish_callback(publish_future, data))
    return publish_future

def publish_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Error: publishing {data} timed out.")
    return callback

if __name__ == "__main__":
    args = sys.argv
    project_id = args[1]
    topic_id = args[2]
    data_list = [
        {"name": "James", "gender": "male", "age": 25}, 
        {"name": "Alice", "gender": "female", "age": 28}
    ]
    publish_futures = []
    for data in data_list:
        publish_futures.append(publishMessage(project_id, topic_id, json.dumps(data).encode("utf-8")))
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    print(f"Published messages with error handler to {project_id}/{topic_id}.")