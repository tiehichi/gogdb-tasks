#!/usr/bin/env python
# encoding: utf-8

import gevent, nodetasks, logging
from gogdbcore.dbmodel import *
from gogdbcore.dataparse import *
from datetime import datetime, timedelta
import re

dlogger = logging.getLogger('GOGDB.DISPATCHER')

def wait_asyncresult(result, timeout):
    dlogger.info('Waitting for task [ %s ]' % (result.id))
    times = 0
    while not result.ready() and times < timeout:
        gevent.sleep(1)
        times += 1


@db_session
def refresh_gamelist_core():
    isSuccessful = False
    asyncgamelist = nodetasks.get_all_game_id.delay()
    dlogger.info('Send Task [ get_all_game_id ] [ %s ]' % (asyncgamelist.id))
    wait_asyncresult(asyncgamelist, 60)
    if not asyncgamelist.ready():
        dlogger.warning('Task [ %s ] Timeout' % (asyncgamelist.id))
        asyncgamelist.forget()
        return isSuccessful
    if asyncgamelist.successful():
        gamelist_parse(asyncgamelist.result)
        asyncgamelist.forget()
        isSuccessful = True
    else:
        dlogger.warning('Task [ %s ] Failed' % (asyncgamelist.id))
        asyncgamelist.forget()
    return isSuccessful


def refresh_gamelist():
    while 1:
        if refresh_gamelist_core():
            gevent.sleep(60 * 60 * 2)
        else:
            gevent.sleep(2)


@db_session
def safe_gamedetail_parse(game_data, lite_mode):
    if '_embedded' in game_data:
        gamedetail_parse(game_data, lite_mode)
    else:
        dlogger.warning('Special Game Data [ %s ]' % (str(game_data)))
        message = game_data['message']
        prodid = max(re.findall('\d+', message), key=len)
        get(gid for gid in GameList if gid.id == prodid).delete()
        dlogger.warning('Remove Product [ %s ]' % prodid)


@db_session
def init_gamedetail_core():
    isIdle = False
    if select(gid for gid in GameList if gid.hasWriteInDB == False).exists():
        ids = select(gid.id for gid in GameList if gid.hasWriteInDB == False)[:100]
    else:
        isIdle = True
        return isIdle

    ids = [ ids[i:i+10] for i in range(0, len(ids), 10) ]
    async_gamesdata = map(nodetasks.get_game_data.delay, ids)
    gevent.joinall([gevent.spawn(wait_asyncresult, agd, 30) for agd in async_gamesdata])
    for agd in async_gamesdata:
        if agd.successful():
            map(lambda gd: safe_gamedetail_parse(gd, lite_mode=True), agd.result)
        agd.forget()

    return isIdle


def init_gamedetail():
    while 1:
        if init_gamedetail_core():
            gevent.sleep(60)
        else:
            gevent.sleep(10)


@db_session
def refresh_gamedetail_core():
    isIdle = False

    if select(game for game in GameDetail
            if datetime.utcnow().replace(tzinfo=None) >= game.lastUpdate + timedelta(hours=6)).exists():
        games = GameDetail.select(lambda game: datetime.utcnow().replace(tzinfo=None) >= game.lastUpdate + timedelta(hours=6)).order_by(GameDetail.lastUpdate)[:100]
        ids = map(lambda g: g.id, games)
    else:
        isIdle = True
        return isIdle

    ids = [ ids[i:i+10] for i in range(0, len(ids), 10) ]
    async_gamesdata = map(nodetasks.get_game_data.delay, ids)
    gevent.joinall([gevent.spawn(wait_asyncresult, agd, 30) for agd in async_gamesdata])
    for agd in async_gamesdata:
        if agd.successful():
            map(lambda gd: safe_gamedetail_parse(gd, lite_mode=False), agd.result)
        agd.forget()

    return isIdle


def refresh_gamedetail():
    while 1:
        if refresh_gamedetail_core():
            gevent.sleep(60)
        else:
            gevent.sleep(10)


@db_session
def refresh_gameprice_core():
    isIdle = False

    timenow = datetime.utcnow().replace(tzinfo=None)
    if select(game for game in GameDetail
            if game.lastPriceUpdate == None
            or game.lastPriceUpdate + timedelta(hours=12) < timenow).exists():
        games = GameDetail.select(lambda game: game.lastPriceUpdate == None
                or game.lastPriceUpdate + timedelta(hours=12) < timenow).order_by(GameDetail.lastPriceUpdate)[:100]
        ids = map(lambda g: g.id, games)
        ids = [ ids[i:i+10] for i in range(0, len(ids), 10) ]
        async_pricedata = map(nodetasks.get_game_global_price.delay, ids)
        gevent.joinall([gevent.spawn(wait_asyncresult, apd, 45) for apd in async_pricedata])
        for apd in async_pricedata:
            if apd.successful():
                map(lambda pd: baseprice_parse(pd), apd.result)
            apd.forget()
    else:
        isIdle = True

    return isIdle


def refresh_gameprice():
    while 1:
        if refresh_gameprice_core():
            gevent.sleep(60 * 5)
        else:
            gevent.sleep(10)


@db_session
def refresh_gamediscount_core():
    isIdle = False

    timenow = datetime.utcnow().replace(tzinfo=None)
    if select(game for game in GameDetail
            if game.lastDiscountUpdate == None
            or game.lastDiscountUpdate + timedelta(hours=2) < timenow).exists():
        games = GameDetail.select(lambda game: game.lastDiscountUpdate == None
                or game.lastDiscountUpdate + timedelta(hours=2) < timenow).order_by(GameDetail.lastDiscountUpdate)[:100]
        ids = map(lambda g: g.id, games)
        ids = [ ids[i:i+10] for i in range(0, len(ids), 10) ]
        async_discountdata = map(nodetasks.get_game_discount.delay, ids)
        gevent.joinall([gevent.spawn(wait_asyncresult, adis, 30) for adis in async_discountdata])
        for adis in async_discountdata:
            if adis.successful():
                map(lambda dis: discount_parse(dis), adis.result)
            adis.forget()
    else:
        isIdle = True

    return isIdle


def refresh_gamediscount():
    while 1:
        if refresh_gamediscount_core():
            gevent.sleep(60)
        else:
            gevent.sleep(10)


if __name__ == '__main__':
    dblite.bind('sqlite', 'dblite.db', create_db=True)
    dblite.generate_mapping(create_tables=True)
    db.bind('postgres', host='127.0.0.1', user='gogdb', password='gogdb', database='gogdb')
    db.generate_mapping(create_tables=True)

    dlogger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    dlogger.addHandler(stream_handler)

    gevent.joinall([
        gevent.spawn(refresh_gamelist),
        gevent.spawn(init_gamedetail),
        gevent.spawn(refresh_gamedetail),
        gevent.spawn(refresh_gameprice),
        gevent.spawn(refresh_gamediscount)])
