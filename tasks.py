"""Create a task for a given queue with an arbitrary payload."""
import logging

from google.cloud import tasks_v2beta3
from google.protobuf import timestamp_pb2

import settings


def insert_task(handler, queue, payload=None):
    client = tasks_v2beta3.CloudTasksClient()

    parent = client.queue_path(settings.GCP_PROJECT_ID,
                               settings.GCP_LOCATION,
                               queue)

    task = {
        'app_engine_http_request': {
            'http_method': 'POST',
            'relative_uri': '/{}'.format(handler)
        }
    }

    if payload is not None:
        # The API expects a payload of type bytes.
        converted_payload = payload.encode()

        # Add the payload to the request.
        task['app_engine_http_request']['body'] = converted_payload

    response = client.create_task(parent, task)

    logging.info('Created task {}'.format(response.name))

    return response


# if in_seconds is not None:
    # # Convert "seconds from now" into an rfc3339 datetime string.
    # d = datetime.datetime.utcnow() + datetime.timedelta(seconds=in_seconds)

    # # Create Timestamp protobuf.
    # timestamp = timestamp_pb2.Timestamp()
    # timestamp.FromDatetime(d)

    # # Add the timestamp to the tasks.
    # task['schedule_time'] = timestamp
