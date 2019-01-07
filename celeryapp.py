from celery import Celery

app = Celery('GOGDB')
app.config_from_object('config')

if __name__ == '__main__':
    app.start()
