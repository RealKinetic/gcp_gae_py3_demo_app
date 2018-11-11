Python 3 Google App Engine Demo App
===================================

### Local

Create a virtual environment for your python application.

```sh
$ mkvirtualenv -a $PWD -p `which python3` py3demoapp
```

Install dependencies locally

``` sh
$ pip install -r requirements.txt
```

Run the app

```
$ python main.py
```

View it in the browswer

``` sh
$ open http://localhost:8080
```

### Manual Deployment

Deploy it

```sh
$ gcloud app deploy
```

Browse it

```sh
$ gcloud app browse
```
