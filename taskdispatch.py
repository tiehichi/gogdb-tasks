#!/usr/bin/env python
# encoding: utf-8

import gevent, nodetasks
from gogdbcore.dbmodel import *
from gogdbcore.dataparse import *
from datatime import datetime, timedelta

def wait_asyncresult(result):
    while not result.ready():
        gevent.sleep(0.5)


def refresh_gamelist():
    while 1:
        asyncgamelist = nodetasks.get_all_game_list.delay()
        wait_asyncresult(asyncgamelist)
        if asyncgamelist.successful():
            gamelist_parse(asyncgamelist.result)
            asyncgamelist.forget()
            gevent.sleep(60 * 60 * 6)
        else:
            asyncgamelist.forget()


def refresh_gamedetail():
    while 1:
        if select(gid for gid in GameList if hasWriteInDB == False).exists():
            ids = select(gid.id for gid in GameList if hasWriteInDB == False)[:100]
            need_lite = True

        elif select(game for game in GameDetail
                if datetime.utcnow().replace(tzinfo=None) >= game.lastUpdate + timedelta(hours=6)).exists():
            ids = select(game.id for game in GameDetail if datetime.utcnow().replace(tzinfo=None) >= game.lastUpdate + timedelta(hours=6))[:100]
            need_lite = False
        else:
            gevent.sleep(10)
            continue

        ids = [ ids[i:i+10] for i in range(0, len(ids), 10) ]
        async_gamesdata = map(nodetasks.get_game_data.delay, ids)
        gevent.joinall([gevent.spawn(wait_asyncresult, agd) for agd in async_gamesdata])
        for agd in async_gamesdata:
            if agd.successful():
                map(lambda gd: gamedetail_parse(gd, lite_mode=need_lite), agd.result)
            agd.forget()

        gevent.sleep(2)
