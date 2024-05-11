from concurrent import futures
from google.cloud import pubsub_v1
import sys

def subscribeMessage(
        project_id: str,
        subscription_id: str,
    ) -> None:

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=subscribe_callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=60)
        except futures.TimeoutError:
            streaming_pull_future.cancel() 
            streaming_pull_future.result()
    print(f"Successfully receive messages with error handler to projects/{project_id}/subscription/{subscription_id}.")

def subscribe_callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received message: \n{message.data.decode()}.")
    message.ack()

  

if  __name__ == "__main__":
    args = sys.argv
    project_id, subscription_id = args[1], args[2]
    subscribeMessage(project_id, subscription_id)