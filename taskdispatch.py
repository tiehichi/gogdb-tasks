#!/usr/bin/env python
# encoding: utf-8

import gevent, nodetasks, logging
from gogdbcore.dbmodel import *
from gogdbcore.dataparse import *
from datetime import datetime, timedelta

dlogger = logging.getLogger('GOGDB.DISPATCHER')

def wait_asyncresult(result):
    dlogger.info('Waitting for task [ %s ]' % (result.id))
    while not result.ready():
        gevent.sleep(0.5)


@db_session
def refresh_gamelist_core():
    isSuccessful = False
    asyncgamelist = nodetasks.get_all_game_id.delay()
    dlogger.info('Send Task [ get_all_game_id ] [ %s ]' % (asyncgamelist.id))
    wait_asyncresult(asyncgamelist)
    if asyncgamelist.successful():
        gamelist_parse(asyncgamelist.result)
        asyncgamelist.forget()
        isSuccessful = True
    else:
        asyncgamelist.forget()
    return isSuccessful


def refresh_gamelist():
    while 1:
        if refresh_gamelist_core():
            gevent.sleep(60 * 60 * 6)
        else:
            gevent.sleep(2)


@db_session
def refresh_gamedetail_core():
    isIdle = False

    if select(gid for gid in GameList if gid.hasWriteInDB == False).exists():
        ids = select(gid.id for gid in GameList if gid.hasWriteInDB == False)[:100]
        need_lite = True

    elif select(game for game in GameDetail
            if datetime.utcnow().replace(tzinfo=None) >= game.lastUpdate + timedelta(hours=6)).exists():
        games = GameDetail.select(lambda game: datetime.utcnow().replace(tzinfo=None) >= game.lastUpdate + timedelta(hours=6)).order_by(GameDetail.lastUpdate)[:100]
        ids = map(lambda g: g.id, games)
        need_lite = False
    else:
        isIdle = True
        return isIdle

    ids = [ ids[i:i+10] for i in range(0, len(ids), 10) ]
    async_gamesdata = map(nodetasks.get_game_data.delay, ids)
    gevent.joinall([gevent.spawn(wait_asyncresult, agd) for agd in async_gamesdata])
    for agd in async_gamesdata:
        if agd.successful():
            map(lambda gd: gamedetail_parse(gd, lite_mode=need_lite), agd.result)
        agd.forget()

    return isIdle


def refresh_gamedetail():
    while 1:
        if refresh_gamedetail_core():
            gevent.sleep(10)
        else:
            gevent.sleep(2)


@db_session
def refresh_gameprice_core():
    timenow = datetime.utcnow().replace(tzinfo=None)
    if select(game for game in GameDetail
            if game.lastPriceUpdate == None
            or game.lastPriceUpdate + timedelta(hours=12) < timenow).exists():
        games = GameDetail.select(lambda game: game.lastPriceUpdate == None
                or game.lastPriceUpdate + timedelta(hours=12) < timenow).order_by(GameDetail.lastPriceUpdate)[:100]
        ids = map(lambda g: g.id, games)
        ids = [ ids[i:i+10] for i in range(0, len(ids), 10) ]
        async_pricedata = map(nodetasks.get_game_global_price.delay, ids)
        gevent.joinall([gevent.spawn(wait_asyncresult, apd) for apd in async_pricedata])
        for apd in async_pricedata:
            if apd.successful():
                map(lambda pd: baseprice_parse(pd), apd.result)
            apd.forget()


def refresh_gameprice():
    while 1:
        refresh_gameprice_core()
        gevent.sleep(15)


@db_session
def refresh_gamediscount_core():
    timenow = datetime.utcnow().replace(tzinfo=None)
    if select(game for game in GameDetail
            if game.lastDiscountUpdate == None
            or game.lastDiscountUpdate + timedelta(hours=2) < timenow).exists():
        games = GameDetail.select(lambda game: game.lastDiscountUpdate == None
                or game.lastDiscountUpdate + timedelta(hours=2) < timenow).order_by(GameDetail.lastDiscountUpdate)[:100]
        ids = map(lambda g: g.id, games)
        ids = [ ids[i:i+10] for i in range(0, len(ids), 10) ]
        async_discountdata = map(nodetasks.get_game_discount.delay, ids)
        gevent.joinall([gevent.spawn(wait_asyncresult, adis) for adis in async_discountdata])
        for adis in async_discountdata:
            if adis.successful():
                map(lambda dis: discount_parse(dis), adis.result)
            adis.forget()


def refresh_gamediscount():
    while 1:
        refresh_gamediscount_core()
        gevent.sleep(5)


if __name__ == '__main__':
    dblite.bind('sqlite', 'dblite.db', create_db=True)
    dblite.generate_mapping(create_tables=True)
    db.bind('postgres', host='127.0.0.1', user='gogdb', password='gogdb', database='gogdb')
    db.generate_mapping(create_tables=True)
    gevent.joinall([
        gevent.spawn(refresh_gamedetail),
        gevent.spawn(refresh_gamelist),
        gevent.spawn(refresh_gameprice),
        gevent.spawn(refresh_gamediscount)])
