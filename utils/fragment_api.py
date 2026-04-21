import aiohttp

from config.config_data import Config, load_config


config: Config = load_config()


async def _get_stars_price(amount: int) -> float:
    url = 'https://tg.parssms.info/v1/stars/price'
    headers = {
        'Content-Type': 'application/json',
        'api-key': config.fragment.api_key
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, ssl=False) as resp:
            data = await resp.json()
            per_star = float(data[0]['approx_price_usd'][1::]) / 50
    return round(amount * per_star, 2)


async def _get_premium_price(amount: int):
    premium_usdt = {
        3: 12,
        6: 16,
        12: 29
    }
    return premium_usdt.get(amount)


async def get_cost(amount: int, type: str):
    if type == 'stars':
        cost = await _get_stars_price(amount)
    else:
        cost = await _get_premium_price(amount)
    usdt_ton = await _get_ton_usdt()
    return round(cost / usdt_ton, 5)


async def _get_ton_usdt() -> float:
    url = 'https://api.coingecko.com/api/v3/coins/the-open-network'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as res:
            resp = await res.json()
            ton = float(resp['market_data']['current_price']['usd'])
    return ton