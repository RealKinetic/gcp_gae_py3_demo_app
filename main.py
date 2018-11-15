import csv
import datetime
import json
import os
import random
import uuid
import time

from flask import Flask
from flask import flash
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for

from google.cloud import bigquery
from google.cloud import datastore
from google.cloud import pubsub_v1
from google.cloud import storage

from werkzeug.utils import secure_filename

import settings
from tasks import insert_task


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)
app.secret_key = b'mysecret'


TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S.000"


def save_message(message):
    ds_client = datastore.Client()
    id_ = uuid.uuid4().hex
    entity = datastore.Entity(key=ds_client.key(settings.MESSAGE_KIND, id_))
    entity.update({
        'message': message,
        'acked': False,
        'timestamp': datetime.datetime.utcnow()
    })

    ds_client.put(entity)

    future = send_message(entity)

    if future:
        future.result()


def send_message(entity):
    if not settings.PUB_SUB_ENALBED:
        app.logger.debug("CLOUD PUB/SUB NOT ENABLED.")
        return

    message = {
        "body": entity["message"],
        "id": entity.key.name
    }
    mjson = json.dumps(message)
    mjson= mjson.encode('utf-8')

    publisher, topic_path = _init_publisher(
        settings.MESSAGE_ENTITY_PUBSUB_TOPIC_NAME)
    return publisher.publish(topic_path, data=mjson)


def get_messages(limit=10):
    ds_client = datastore.Client()
    query = ds_client.query(kind=settings.MESSAGE_KIND)
    query.order = ['-timestamp']
    return query.fetch(limit=limit)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        message = request.form['user_message']

        if not message:
            flash('Message body cannot be empty.', 'error')
            return redirect(url_for('index'))

        save_message(message)

        flash('Message sent successfully.')
        return redirect(url_for('index'))

    try:
        messages = get_messages()
    except Exception as e:
        app.logger.exception(e)
        flash('Unable to load messages.')
        messages = []

    return render_template('index.html', messages=messages)


@app.route('/_config', methods=['GET'])
def config():
    return render_template('config.html', env_vars=os.environ.items())


@app.route('/csvupload', methods=['GET', 'POST'])
def csvupload():
    if request.method == 'POST':
        f = request.files['csvfile']
        upload_file_to_cloud_storage(settings.CLOUD_STORAGE_BUCKET,
                                     secure_filename(f.filename),
                                     f)
        flash('File uploaded successfully.')
        return redirect(url_for('csvupload'))

    return render_template('file_upload.html', env_vars=os.environ.items())


def upload_file_to_cloud_storage(bucket_id, file_name, file_):
    client = storage.Client()
    bucket = client.get_bucket(bucket_id)
    blob = bucket.blob(file_name)
    # blob.upload_from_string('New contents!')
    blob.upload_from_file(file_)


@app.route('/dataflow', methods=['GET', 'POST'])
def dataflow():
    if request.method == 'POST':
        name = request.form['name']
        run_dataflow_job(name)
        flash('Dataflow job {} started successfully.'.format(name))
        return redirect(url_for('dataflow'))

    return render_template('dataflow.html', env_vars=os.environ.items())


def run_dataflow_job(name):
    from googleapiclient.discovery import build
    from oauth2client.client import GoogleCredentials

    credentials = GoogleCredentials.get_application_default()
    service = build('dataflow', 'v1b3', credentials=credentials)

    # Set the following variables to your values.
    PROJECT = settings.GCP_PROJECT_ID
    BUCKET = settings.CLOUD_STORAGE_BUCKET
    TEMPLATE = 'dataflowtemplates'

    GCSPATH="gs://{bucket}/templates/{template}".format(bucket=BUCKET, template=TEMPLATE)
    BODY = {
        "jobName": "{jobname}".format(jobname=name),
        "parameters": {
            # "inputFile": "gs://{bucket}/input/my_input.txt",
            # "outputFile": "gs://{bucket}/output/my_output".format(bucket=BUCKET)
        },
        "environment": {
            # "tempLocation": "gs://{bucket}/temp".format(bucket=BUCKET),
            "zone": "us-central1-f"
        }
    }

    request = service.projects().templates().launch(projectId=PROJECT, gcsPath=GCSPATH, body=BODY)
    response = request.execute()

    print(response)


GROUPS = ["GROUP_{}".format(val) for val in range(10)]


USERS = [
    "Carrie Baxter",
    "Lila Ramos",
    "Donny Pillar",
    "Leon Mays",
    "Eugene Hargrove",
    "Ayanna Kenny",
    "Kevin Cofield",
    "Anna Smith",
    "Andrew Stone",
    "Rhonda Mcmanis",
    "Antonio Goodale",
    "Terry Gonzalez",
    "Nicholas Slane",
    "Richard Francis",
    "Ariel Morton",
    "Judy Wooten",
    "Mary Nelson",
    "Jack Hyatt",
    "Rena Bayne",
    "Ivy Lee",
    "Sheila Hernandez",
    "Peter Stowers",
    "David Hogen",
    "Oscar Magar",
    "Walter Gray",
    "Polly Sanders",
    "Benito Street",
    "Mary Hunn",
    "Robert Brown",
    "Bob Quale",
    "Gilda Bertholf",
    "Steven Roller",
    "Carol Friou",
    "Gisela Akers",
    "Mary Myers",
    "Sandra Bevins",
    "Robert Reeves",
    "Mallory Anderson",
    "Harold Richmond",
    "Charmaine Sanchez",
    "Emma Mederios",
    "Luciano Randall",
    "Joshua Malone",
    "Harriet Wilkins",
    "Michael Oneil",
    "Margaret Eaton",
    "Sandra Pena",
    "Kathy Parker",
    "Madeline Sullivan",
    "Gregory Fox",
    "Stephanie Cadle",
    "William Richards",
    "Helen Iverson",
    "George Gray",
    "Jessica Postley",
    "Linda Bussey",
    "Kathy Woods",
    "Kurt Gonzalez",
    "Elvera Pittman",
    "Robert Batts",
    "Paul Woods",
    "Dorothy Tofil",
    "Sara Munoz",
    "Steven Brown",
    "Lisa Harling",
    "Robert Newman",
    "Rebecca Mullen",
    "Frederick Davis",
    "Lula Squires",
    "Henry Macmillan",
    "Alfred Rutland",
    "Antoinette Deppe",
    "Jaclyn Wigington",
    "Mark Watson",
    "John Osborne",
    "Thomas Sand",
    "Elizabeth Fair",
    "Paul Cross",
    "Regina Jennings",
    "Elizabeth Alvarez",
    "Gloria Thorpe",
    "Sherry Henry",
    "Barbara Chambers",
    "Thomas Hawkes",
    "Harold Sanchez",
    "Nathan Kopple",
    "Iva Gunter",
    "Jeffrey Houser",
    "Rickey Mctaggart",
    "Guy Wilt",
    "Eric Harvey",
    "Steven Ackerman",
    "Joy Sprague",
    "Diane Hernandez",
    "Judith Perez",
    "Gretchen Swann",
    "James Ocampo",
    "Joyce Kattner",
    "Laura Buran",
    "Joan Walker",
    "Beau Cunningham",
    "John Wayne",
    "Bobo Fett"
]


@app.route('/tasks', methods=['GET', 'POST'])
def tasks():
    if request.method == 'POST':
        number_of_groups = request.form['number_of_groups']
        number_of_users = request.form['number_of_users']
        payload = json.dumps({
            'number_of_groups': number_of_groups,
            'number_of_users': number_of_users
        })
        insert_task('value_task_handler', settings.TASK_QUEUE_NAME, payload)
        flash('Tasks inserted for {} Groups with {} users.'.format(
            number_of_groups, number_of_users))

        return redirect(url_for('tasks'))

    return render_template('tasks.html', env_vars=os.environ.items())


@app.route('/value_task_handler', methods=['POST'])
def value_task_handler():
    payload = json.loads(request.get_data(as_text=True))

    number_of_groups = int(payload["number_of_groups"])

    for g in range(number_of_groups):
        p = json.dumps({
            "group": GROUPS[g % 10],
            "number_of_users": payload["number_of_users"]
        })
        insert_task('group_values', settings.TASK_QUEUE_NAME, p)

    return 'Printed task payload: {}'.format(payload)


@app.route('/group_values', methods=['POST'])
def group_values():
    payload = json.loads(request.get_data(as_text=True))

    group = payload["group"]
    number_of_users = int(payload["number_of_users"])

    v, r = divmod(number_of_users, 100)

    if v == 0:
        v = 1

    if r == 0:
        r = 1

    for u in range(v):
        app.logger.info("GROUP: {}".format(group))
        p = json.dumps({
            "group": group,
            "number_of_users": r
        })
        app.logger.info("GROUP: {}, # OF USERS: {}".format(group, r))
        insert_task('user_values', settings.TASK_QUEUE_NAME, p)

    return 'Printed task payload: {}'.format(payload)


@app.route('/user_values', methods=['POST'])
def user_values():
    payload = json.loads(request.get_data(as_text=True))

    group = payload["group"]
    number_of_users = int(payload["number_of_users"])

    for u in range(number_of_users):
        user = USERS[random.randint(0, 99)]
        app.logger.info("GROUP: {}, USER: {}".format(group, user))
        insert_value_message(group, user)

    return 'Printed task payload: {}'.format(payload)


def insert_value_message(group, user):
    message = "{},{},{},{},{}".format(
        user, group, random.randint(0, 100000), int(time.time() * 1000),
        datetime.datetime.now().strftime(TIMESTAMP_FMT))

    mjson= message.encode('utf-8')

    publisher, topic_path = _init_publisher(settings.MESSAGE_PUBSUB_TOPIC_NAME)
    return publisher.publish(topic_path, data=mjson)


USER_QUERY = """
SELECT * FROM `{}.{}.high_values_users`
ORDER BY update_time DESC
LIMIT 1000""".format(
    settings.GCP_PROJECT_ID, settings.BQ_DATASET_ID)


# SELECT * FROM `{}.{}.high_values_groups` LIMIT 1000
GROUP_QUERY = """
SELECT g.group,
       sum(g.total_value) as total_value,
       max(g.processing_time) as processing_time,
       g.window_start
FROM `{}.{}.high_values_groups` g
GROUP BY  g.group, g.window_start
ORDER BY g.window_start DESC
LIMIT 1000
""".format(
    settings.GCP_PROJECT_ID, settings.BQ_DATASET_ID)


@app.route('/bigquery_results', methods=['GET'])
def bigquery_results():
    user_results = _query_bq(USER_QUERY)
    group_results = _query_bq(GROUP_QUERY)

    return render_template('bigquery.html', user_results=user_results.result(),
                           group_results=group_results.result())


def _query_bq(query):
    bq_client = bigquery.Client()
    # dataset = bigquery.Dataset(bq_client.dataset(dataset_id))

    job = bq_client.query(query)

    return job


def _init_publisher(topic_name):
    publisher = pubsub_v1.PublisherClient()
    return publisher, publisher.topic_path(settings.GCP_PROJECT_ID, topic_name)


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
