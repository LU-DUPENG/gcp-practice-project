**Chapter 1. What is Pub/Sub**
---

Pub/Sub, offered by Google Cloud, is a messaging service that enables asynchronous communication between applications. It follows a **topic-subscription model** for sending and receiving messages.

 1. **Topics:** These act as named channels or categories. Publishers send messages to these topics.
 2.   **Subscriptions:** These are filters attached to specific topics. Subscribers interested in receiving messages from a topic create subscriptions for it.
 3.   **Publishers:** These are applications or services that generate data and send messages to topics.
 4.   **Subscribers:** These are applications or services that listen for messages published to topics they are subscribed to. Messages are delivered to subscribers **in the order they are published**.
 5.   **Message Delivery:** Subscribers can **pull** or **push** messages from Pub/Sub. Additionally, they need to **acknowledge** receiving messages to inform Pub/Sub that they have been processed successfully.
 6.   **Supported Communication Patterns:** Pub/Sub facilitates various communication patterns, including:
 **One-to-many:** A single publisher sends messages to multiple subscribers.
 **Many-to-one:** Multiple publishers send messages to a single subscriber.
 **Many-to-many:** Multiple publishers send messages to multiple subscribers.

  

 **Chapter 2. How to use Pub/Sub in Python**
---
**Section 2-1. Prerequisites**
 1. **Set up Google Cloud CLI environment:** Follow the instructions here to set up the Google Cloud CLI
[Publish and receive messages in Pub/Sub by using a client library](https://cloud.google.com/pubsub/docs/publish-receive-messages-client-library#before-you-begin)
 2. **Set up Python environment:** Ensure you have a working Python environment with the necessary libraries installed. Refer to the guide here for setting up a Python environment
[Setting up a Python development environment](https://cloud.google.com/python/docs/setup)

**Section 2-2. Publishing Messages**

 3. **Importing libraries**
 -   `concurrent.futures`: For handling asynchronous tasks.
-   `google.cloud.pubsub_v1`: For interacting with the Pub/Sub API.
-   `typing`: For type hints (optional but improves code readability).
-   `json`: For handling JSON data (if applicable).
-   `sys`: For accessing command-line arguments.

```python
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
import json
import sys
```

 4. **Creating a Publisher Client**
The code snippet creates a `pubsub_v1.PublisherClient` object to interact with the Pub/Sub API. It then constructs the topic path using `publisher.topic_path(project_id, topic_id)`.
```python
# project-id and topic-id are passed by function arguments
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
```

5. **Publishing Data**
-   The function `publishMessage` takes `project_id`,  `topic_id`, and `data` as arguments.
-   It creates a `publish_future` object using `publisher.publish(topic_path, data)`. This future object represents the asynchronous publishing task.
-   The `publish_future.add_done_callback` method attaches a callback function (`publish_callback`) to handle the future's completion.

The `publish_callback` function checks the outcome of the publishing operation:

-   If successful, it prints a confirmation message.
-   In case of a timeout error, it indicates that the message publishing timed out.

**Explanation of Blocking vs Non-Blocking Behavior:**

-   `publish_future.result()` blocks the program execution until the message is published or a timeout occurs.
-   `publish_future.add_done_callback` allows the program to continue execution without waiting for the message to be published. The callback function is invoked when the publishing operation completes.

```python
# data is passed by function arguments
publish_future = publisher.publish(topic_path, data)
publish_future.add_done_callback(publish_callback(publish_future, data))

def publish_callback(
	publish_future: pubsub_v1.publisher.futures.Future,
	data: str
	) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
	def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
		try:
			publish_future.result(timeout=60)
		except futures.TimeoutError:
			print(f"TimeoutError: Publishing {data} time out.")
	return callback 
```

6. **Complete Program** (`publishMessage.py`)

```python: publishMessage.py
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
import json
import sys

def publishMessage(
        project_id: str, 
        topic_id: str, 
        data: str
    ) -> pubsub_v1.publisher.futures.Future:

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    publish_future = publisher.publish(topic_path, data)
    publish_future.add_done_callback(publish_callback(publish_future, data))

    return publish_future

def publish_callback(
    publish_future: pubsub_v1.publisher.futures.Future, 
    data: str
    ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:

    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Error: publishing {data} timed out.")

    return callback



def  publishMessageList(
        project_id: str,
        topic_id: str,
        data_list: list
    ) -> None:

    publish_futures = []

    for data in data_list:
        publish_futures.append(publishMessage(project_id, topic_id, json.dumps(data).encode("utf-8")))

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    print(f"Successfully publish message list with error handler to projects/{project_id}/topics/{topic_id}.")


if __name__ == "__main__":
    args = sys.argv
    project_id = args[1]
    topic_id = args[2]
    data_list = [
        {"name": "James", "gender": "male", "age": 25}, 
        {"name": "Alice", "gender": "female", "age": 28}
    ]
    publishMessageList(project_id, topic_id, data_list)

```

  

**Section 2-3. Subscribing to Messages**
1. **Create subscriber client**
Similar to the publisher, the code snippet creates a `pubsub_v1.SubscriberClient` object to interact with Pub/Sub for subscribing. It then constructs the subscription path using `subscriber.subscription_path(project_id, subscription_id)`.

```python
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
```

2. **Pulling Messages and Acknowledging Them**
The `subscribe_callback` function is invoked whenever a message is received on the subscription.
-   It decodes the message data and prints it.
-   It calls `message.ack()` to acknowledge that the message has been received successfully.

```python
def subscribe_callback(message: pubsub_v1.subscriber.message.Message) -> None:
	print(f"Received message: \n{message.data.decode()}.")
	message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=subscribe_callback)
```

3. **Complete Program**(`subscribeMessage.py`)

```python: subscribeMessage.py
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
```
  

**Chapter 3. Some Usecases**
---

**Section 3-1. Write Messages to BigQuery Using BigQuery Subscriptions**

This approach leverages BigQuery subscriptions, a feature that simplifies data ingestion from Pub/Sub topics directly into BigQuery tables. Here's a step-by-step guide:

1.  **Set Up BigQuery**
-   Set up a BigQuery dataset and table to store the incoming data. For example, you could create a dataset named `my_pubsub_data` and a table named `pub_sub_messages` within it.
-   Define the table schema to match the structure of your Pub/Sub message data. The schema specifies the data types (e.g., string, integer) for each field in your messages. You can find information on defining schemas here:
Reference: [Specifying a schema](https://cloud.google.com/bigquery/docs/schemas)

2. **Create a Topic Schema**
- Use the Google cloud console or gcloud command-line tool to create a schema used by topic message.
Reference: [Create a schema for a topic](https://cloud.google.com/pubsub/docs/create-schema)
  
3.  **Create a Pub/Sub Topic**
-   Use the Google Cloud Console or `gcloud` to create a topic. This topic will act as the source for data flowing into BigQuery.
-   During topic creation, ensure you select the option to "Use a schema" (if you created one in step 2) and choose JSON as the message encoding format. This ensures compatibility with BigQuery Subscriptions. Refer to this documentation for associating a schema with a topic:
Reference: [Associate a schema with a topic](https://cloud.google.com/pubsub/docs/associate-schema-topic)

4.  Create a BigQuery Subscription
In the Google Cloud Console or using `gcloud`, create a BigQuery subscription. Specify the following details:
	-   **Subscription Name:** Choose a descriptive name for your subscription.
	-   **Project ID:** The project ID containing the BigQuery dataset and table (from step 1).
	-   **Dataset ID:** The ID of the dataset where the BigQuery table resides (from step 1).
	-   **Table ID:** The ID of the table that will receive the data (from step 1).
	-   **Schema Option:** Select "Use topic schema" to leverage the schema defined for your Pub/Sub topic (if applicable).

5. **Publish Messages**
Use the `publishMessage.py` script (or any other method) to publish messages to the Pub/Sub topic. Ensure the message data structure aligns with the defined schema (if any).