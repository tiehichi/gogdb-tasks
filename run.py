from pony.orm import *
import tasks
from helper import dbhelper, bot
import logging
from gogdbcore.dbmodel import db, Game

import time
from gogdbcore.gogprice import Countries
from datetime import timedelta

from config import DB_PROVIDER, DB_USER, DB_PASS, DB_HOST


logging.basicConfig(level=logging.WARNING, format="%(name)s - %(funcName)s - %(levelname)s - %(message)s")
logger = logging.getLogger('GOGDB')
db.bind(provider=DB_PROVIDER, user=DB_USER, password=DB_PASS, host=DB_HOST, database='gogdb')
db.generate_mapping(create_tables=True)

countries = tasks.get_countries()
products = tasks.get_products()

logger.warning(f"products count: {tasks.get_products_num()}")

for page in products:
    dbhelper.init_products(page)
logger.warning("initlalized products table")

dbhelper.save_data(countries)

while True:
    uninit = dbhelper.uninited_visible_products()
    logger.warning(f'Need Init: {uninit}')
    if len(uninit) != 0:
        products_data = tasks.get_products_data(uninit)
        dbhelper.save_data(products_data)
        time.sleep(2)
    else:
        break

while True:
    uninit = dbhelper.products_builds_need_init()
    logger.warning(f'Init Builds: {uninit}')
    if len(uninit) != 0:
        products_data = tasks.get_products_builds(uninit, ['windows', 'osx'])
        dbhelper.save_data(products_data)
        time.sleep(2)
    else:
        break

while True:
    uninit = dbhelper.products_price_need_init()
    logger.warning(f'Init Price: {uninit}')
    if len(uninit) != 0:
        products_data = tasks.get_products_prices(uninit, ['us', 'cn'])
        dbhelper.save_data(products_data)
        time.sleep(2)
    else:
        break

while True:
    uninit = dbhelper.products_achievements_need_init()
    logger.warning(f'Init Achievements: {uninit}')
    if len(uninit) != 0:
        products_data = tasks.get_products_achievements(uninit, bot.user_id, bot.token_type, bot.access_token)
        dbhelper.save_data(products_data)
        time.sleep(2)
    else:
        break

