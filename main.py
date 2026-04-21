import asyncio
import logging

import uvicorn
from fastapi import FastAPI, Request, HTTPException, Response

from models import SendRequest
from config.config_data import Config, load_config
from logic.process import process_message
from logic.processors.aggregator import QueueManager
from logic.distribute import distribute_endpoint
from logic.wallets.manager import WalletStorage


app = FastAPI()


format = '[{asctime}] #{levelname:8} {filename}:{lineno} - {name} - {message}'

logging.basicConfig(
    level=logging.DEBUG,
    format=format,
    style='{'
)


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": format,
            "style": "{",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"level": "INFO", "handlers": ["default"], "propagate": False},
        "uvicorn.access": {"level": "INFO", "handlers": ["default"], "propagate": False},
        "": {"handlers": ["default"], "level": "DEBUG", "propagate": False},  # корневой логгер
    },
    "root": {
        "level": "DEBUG",
        "handlers": ["default"],
    },
}


logger = logging.getLogger(__name__)

config: Config = load_config()


@app.post("/buy")
async def process_send(resp: Request, msg: SendRequest):
    logger.info(f"Start process message: {msg.currency}, {msg.type}")
    try:
        result: dict = await process_message(msg, resp.app.state.wallets, resp.app.state.queues)
    except Exception as err:
        logger.error(f'Critical error during transaction execute: {err}')
        return {
            'ok': False,
            'message': err
        }
    if not result.get('status'):
        logger.error(f'Failed process message: {result.get("message")}')
        return {
            'ok': False,
            'message': result.get("message")
        }
    return {
        'ok': True,
        'message': ''
    }


@app.post('/distribute')
async def distribute_wallets(req: Request):
    print('here')
    try:
        result = await distribute_endpoint(req.app.state.wallets)
        return {
            'ok': result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def main():
    logger.info('Start configurate managers')
    wallet_storage = WalletStorage()
    queue_manager = QueueManager(wallet_storage)
    #print(config.wallet.seed_phrase)
    await queue_manager.init_queue()

    app.state.wallets = wallet_storage
    app.state.queues = queue_manager

    logger.info('Start transfer system')

    uvicorn_config = uvicorn.Config(app, host='0.0.0.0', port=8090, log_level="info", log_config=LOGGING_CONFIG)  # ssl_keyfile='ssl/key.pem', ssl_certfile='ssl/cert.pem'
    server = uvicorn.Server(uvicorn_config)
    try:
        print('success')
        await server.serve()
    except Exception:
        ...
    finally:
        logger.info('Stop fragment system')


if __name__ == '__main__':
    asyncio.run(main())
