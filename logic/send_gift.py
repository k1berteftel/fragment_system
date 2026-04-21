from pyrogram import Client

from config.config_data import Config, load_config

config: Config = load_config()

app = Client('app', api_id=config.user_bot.api_id, api_hash=config.user_bot.api_hash)


async def send_gift(username: str, gift_id: int):
    async with app:
        try:
            msg = await app.send_gift(
                chat_id=username,
                gift_id=gift_id,
                is_private=True
            )
            return True
        except Exception as err:
            raise err
    return False