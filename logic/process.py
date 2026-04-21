import asyncio
import random
import logging

from models import SendRequest, Wallet, AggregatorStatus
from logic.wallets.manager import WalletStorage
from logic.processors.fast import process_fastlane
from logic.processors.aggregator import QueueManager
from logic.send_gift import send_gift
from utils.fragment_api import get_cost


logger = logging.getLogger(__name__)


def polling_task(task_id, queues: QueueManager) -> dict | None:
    tasks = queues.get_tasks()
    for task in tasks:
        if task.get('id') == task_id:
            if task.get('status') == AggregatorStatus.COMPLETED.value:
                queues.del_task(task_id)
                return {
                    'status': True
                }
            if task.get('status') == AggregatorStatus.FAILED.value:
                return {
                    'status': False,
                    'message': task.get('error')
                }
    return None


async def _get_free_wallet(cost: float, wallet_storage: WalletStorage) -> dict | None | Wallet:
    wallets = wallet_storage.get_wallets()
    enough = False
    for wallet in wallets:
        if wallet.balance > cost:
            enough = True

    if not enough:
        sum = 0
        for wallet in wallets:
            sum += wallet.balance
        if sum < cost:
            return {
                'status': False,
                'message': "Enough balance"
            }
        else:
            return None
    free_wallets = []
    for wallet in wallets:
        if wallet.status == 'free' and wallet.balance > cost:
            free_wallets.append(wallet)
    if not free_wallets:
        await asyncio.sleep(2.5)
        return await _get_free_wallet(cost, wallet_storage)
    return max(free_wallets, key=lambda x: x.balance)


async def process_message(msg: SendRequest, wallet_storage: WalletStorage, queues: QueueManager) -> dict:
    if msg.type == 'deleted_gift':
        try:
            result = await send_gift(msg.username, msg.currency)
        except Exception as err:
            return {
                'status': False,
                'message': err
            }
        return {
            'status': result
        }

    cost = await get_cost(msg.currency, msg.type)

    logger.info("Choosing target wallet...")

    while True:
        result = await _get_free_wallet(cost, wallet_storage)
        if isinstance(result, dict):
            return result
        else:
            break

    if result is None:
        logger.info('Add queue task')
        task_id = queues.add_task(msg, cost)
        logger.info('Start polling task')
        while True:
            status = polling_task(task_id, queues)
            if status is None:
                await asyncio.sleep(3)
                continue
            break
    else:
        logger.info('Start fastlane process')
        wallet = result
        status = await process_fastlane(msg, wallet, wallet_storage)
    return status
