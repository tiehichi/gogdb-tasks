import asyncio
from gogdbcore.gogprice import Countries, GOGPrice
from gogdbcore.gogapi import gogapi
from gogdbcore.gogproduct import GOGProduct
from gogdbcore.gogbuildsystem import BuildsTable
from gogdbcore.gogachievement import AchievementsTable


def get_countries():
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(Countries.create())


def get_products():
    loop = asyncio.get_event_loop()
    try:
        return loop.run_until_complete(gogapi.get_all_product_id())
    except Exception as e:
        return e


def get_products_num():
    loop = asyncio.get_event_loop()
    try:
        return loop.run_until_complete(gogapi.get_total_num())
    except Exception as e:
        return e


def get_products_data(products: list):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(GOGProduct.create_multi(products))


def get_products_prices(products: list, countries: list):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(GOGPrice.create_multi(products, countries))


def get_products_builds(products: list, os: list):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(BuildsTable.create_multi(products, os))


def get_products_achievements(client_ids: list, user_id: str, token_type: str, access_token: str):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(AchievementsTable.create_multi(client_ids, user_id, token_type, access_token))
