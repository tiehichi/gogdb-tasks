broker_url = 'amqp://{user}:{passwd}@{ip}:{port}/gogdb_vhost'
result_backend = 'redis://:{password}@{ip}:{port}'
task_serializer = 'msgpack'
result_serializer = 'json'
result_expires = 60 * 60
accept_content = ['json', 'msgpack']
enable_utc = True
imports = ['nodetasks']
