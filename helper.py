from pony.orm import *
from gogdbcore.dbmodel import Game, DefaultClientInfo
from gogdbcore.gogexceptions import GOGProductNotFound
from datetime import timedelta, datetime

from gogdbcore.gogbase import GOGBase
from gogdbcore.gogprice import Countries, GOGPrice
from gogdbcore.gogproduct import GOGProduct
from gogdbcore.gogbuildsystem import BuildsTable
from gogdbcore.gogachievement import AchievementsTable
from gogdbcore.gogtoken import GOGToken
from gogdbcore.gogapi import gogapi

import logging
import os
import asyncio
from enum import Enum

from config import BOT_USERNAME, BOT_PASSWORD


class CheckoutType(Enum):
    DETAIL = 1
    PRICE = 2
    BUILDS = 3


class DBHelper:
    
    def __init__(self):
        self.logger = logging.getLogger('GOGDB.DBHelper')
        self.__save_data_func_table = {
            Countries: self.__save_data,
            GOGProduct: self.__save_data,
            GOGPrice: self.__save_data,
            BuildsTable: self.__save_data,
            AchievementsTable: self.__save_data,
            Exception: self.__deal_exception_data,
            list: self.__data_in_list,
        }

    @db_session(strict=True)
    def init_products(self, products_id: list):
        list(map(lambda x: Game.save_into_db(**{"id":x}), products_id))

    @db_session(strict=True)
    def uninited_visible_products(self, limit: int=10):
        return select(prod.id for prod in Game if prod.initialized == False and \
            prod.invisible == False)[:limit]

    @db_session(strict=True)
    def uninited_products(self, limit: int=10):
        return select(prod.id for prod in Game if prod.initialized == False)[:limit]

    @db_session(strict=True)
    def products_price_need_init(self, limit: int=10):
        return select(prod.id for prod in Game if prod.priceCheckout is None and \
            prod.initialized == True and prod.invisible == False)[:limit]

    @db_session(strict=True)
    def products_builds_need_init(self, limit: int=10):
        return select(prod.id for prod in Game if prod.buildsCheckout is None and \
            prod.initialized == True and prod.invisible == False)[:limit]

    @db_session(strict=True)
    def products_achievements_need_init(self, limit: int=10):
        return select(client_info.clientId for client_info in DefaultClientInfo \
            if client_info.achievementsTable is None)[:limit]

    @db_session(strict=True)
    def __products_detail_need_checkout(self, limit, delta_time):
        prods = Game.select(lambda prod: datetime.utcnow().replace(tzinfo=None) >= \
            prod.detailCheckout + delta_time).order_by(Game.detailCheckout)[:limit]
        return [prod.id for prod in prods]

    @db_session(strict=True)
    def __products_price_need_checkout(self, limit, delta_time):
        prods = Game.select(lambda prod: datetime.utcnow().replace(tzinfo=None) >= \
            prod.priceCheckout + delta_time).order_by(Game.priceCheckout)[:limit]
        return [prod.id for prod in prods]

    @db_session(strict=True)
    def __products_builds_need_checkout(self, limit, delta_time):
        prods = Game.select(lambda prod: datetime.utcnow().replace(tzinfo=None) >= \
            prod.buildsCheckout + delta_time).order_by(Game.buildsCheckout)[:limit]
        return [prod.id for prod in prods]

    def products_need_checkout(self, checkout_type: CheckoutType, limit: int=10, delta_time: timedelta=timedelta(hours=6)):
        return {
            CheckoutType.BUILDS: self.__products_builds_need_checkout,
            CheckoutType.DETAIL: self.__products_detail_need_checkout,
            CheckoutType.PRICE: self.__products_price_need_checkout,
        }[checkout_type](limit, delta_time)

    @db_session(strict=True)
    def __save_data(self, data: GOGBase):
        data.save_or_update()

    @db_session(strict=True)
    def __deal_exception_data(self, data: Exception):
        if isinstance(data, GOGProductNotFound):
            Game[data.product_id].invisible = True
        else:
            return

    def __data_in_list(self, data):
        list(map(lambda x: self.save_data(x), data))

    def save_data(self, data):
        if isinstance(data, Exception):
            self.__save_data_func_table[Exception](data)
        else:
            self.__save_data_func_table[type(data)](data)


dbhelper = DBHelper()


class BotHelper:

    def __init__(self, username: str, password: str):
        self.__token = GOGToken()
        if not os.path.exists(self.__token.token_file):
            event_loop = asyncio.get_event_loop()
            token_data = event_loop.run_until_complete(gogapi.login(username, password))
            self.__token.load(**token_data)
        else:
            self.__token.load_from_file(self.__token.token_file)

    @property
    def access_token(self):
        if self.__token.is_expired():
            self.__token.refresh()
        return self.__token.access_token

    @property
    def token_type(self):
        return self.__token.token_type

    @property
    def user_id(self):
        return self.__token.user_id

    @property
    def refresh_token(self):
        return self.__token.refresh_token


bot = BotHelper(BOT_USERNAME, BOT_PASSWORD)
