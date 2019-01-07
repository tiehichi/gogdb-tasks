from celery.schedules import crontab

broker_url = 'amqp://gogdb:gogdb@192.168.3.169/gogdb_vhost'
result_backend = 'redis://192.168.3.169'
task_serializer = 'msgpack'
result_serializer = 'json'
result_expires = 60 * 60
accept_content = ['json', 'msgpack']
enable_utc = True
imports = ['servertasks']

beat_schedule = {
    'refresh_gamelist':{
        'task': 'servertasks.refresh_gamelist',
        'schedule': 10.0,
        'args': ()
    }
}

