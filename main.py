import csv
import datetime
import json
import os
import uuid

from flask import Flask
from flask import flash
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for

from google.cloud import datastore
from google.cloud import pubsub_v1
from google.cloud import storage

from werkzeug.utils import secure_filename

import settings


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)
app.secret_key = b'mysecret'

ds_client = datastore.Client()
publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(settings.GCP_PROJECT_ID,
                                  settings.MESSAGE_PUBSUB_TOPIC_NAME)


def save_message(message):
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
    return publisher.publish(topic_path, data=mjson)


def get_messages(limit=10):
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

    messages = get_messages()

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


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
