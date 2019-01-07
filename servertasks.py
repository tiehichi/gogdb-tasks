from gogdbcore.dbmodel import *
import nodetasks
from celeryapp import app
from gogdbcore.dataparse import *

dblite.bind('sqlite', 'dblite.db', create_db=True)
dblite.generate_mapping(create_tables=True)

db.bind('postgres', host='127.0.0.1', user='gogdb', password='gogdb', database='gogdb')
db.generate_mapping(create_tables=True)

@app.task(ignore_result=True)
def refresh_gamelist():
    async_result = nodetasks.get_all_game_id.delay()
    while not async_result.ready():
        pass
    if async_result.successful():
        gamelist_parse(async_result.result)
    async_result.forget()
