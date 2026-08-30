import asyncio
import logging
import threading
import json
from pathlib import Path
from datetime import datetime
from typing import Literal
import uuid

from logic.wallets.manager import WalletStorage
from logic.processors.fast import process_fastlane
from utils.transactions import collect_funds_from_seeds_string
from utils.distribution import distribute_collection
from sys_types import SendRequest, Wallet, AggregatorStatus


RESERVE = 0.1


logger = logging.getLogger(__name__)


class QueueManager:
    def __init__(self, wallet_storage: WalletStorage, path: str = "queue.json"):
        self.queue_file = Path(path)
        self.lock = threading.RLock()
        self.wallet_storage = wallet_storage

    async def init_queue(self):
        if not self.queue_file.exists():
            with open(self.queue_file, 'w') as f:
                json.dump({
                    "tasks": []
                }, f, indent=2)
        asyncio.create_task(self._polling_task())

    async def _polling_task(self):
        while True:
            tasks: list[dict] = self._read_queue().get('tasks')
            task = None
            for t in tasks[::-1]:
                if t.get('status') == AggregatorStatus.PENDING.value:
                    task = t
                    break

            if not task:
                await asyncio.sleep(3)
                continue
            task['status'] = AggregatorStatus.PROCESSING.value
            self._update_task(task)

            logger.info('The task was successfully selected')
            logger.info('Start sorting wallets')

            sum = 0
            wallets = self.wallet_storage.get_wallets()
            collected_wallets = []
            for wallet in wallets:
                if wallet.status == 'free':
                    sum += wallet.balance
                    collected_wallets.append(wallet)

            collected_wallets.sort(key=lambda x: x.balance)
            target_wallet = collected_wallets.pop(0)

            logger.info(f'Target wallet balance: {target_wallet.balance}')

            if sum < task.get('cost'):
                logger.info(f'Collected sum: {sum}. Need: {task.get("cost")}')
                task['status'] = AggregatorStatus.PENDING.value
                self._update_task(task)
                logger.info('Wait for 3 sec before continue')
                await asyncio.sleep(3)
                continue


            sum = 0
            donor_wallets = []
            for wallet in collected_wallets[::-1]:
                sum += wallet.balance
                donor_wallets.append(wallet)
                if sum >= task.get('cost') + RESERVE:
                    break

            if sum < task.get('cost'):
                logger.info('The amount of balances is not enough for aggregate')
                task['error'] = "The amount of balances is not enough for aggregate"
                task['status'] = AggregatorStatus.FAILED.value
                self._update_task(task)
                await asyncio.sleep(3)
                continue

            await self.set_wallets_status([target_wallet, *collected_wallets], 'sync')

            target_amount = task.get('cost') - target_wallet.balance
            source_wallets = distribute_collection(target_amount, donor_wallets)

            if not source_wallets:
                logger.error('Error during distribution donor wallets')
                task['error'] = "Calculate distribution donor wallets"
                task['status'] = AggregatorStatus.FAILED.value
                self._update_task(task)
                await self.set_wallets_status([target_wallet, *collected_wallets], 'free')
                await asyncio.sleep(3)
                continue

            result = await collect_funds_from_seeds_string(
                target_wallet.address,
                source_wallets,
                target_amount
            )
            if not result.get('status'):
                logger.error('Error during fundraising')
                task['error'] = result.get('error')
                task['status'] = AggregatorStatus.FAILED.value
                self._update_task(task)
                await self.set_wallets_status([target_wallet, *collected_wallets], 'free')
                await self.update_wallets_balance(wallets)
                await asyncio.sleep(3)
                continue

            await self.update_wallets_balance([target_wallet, *collected_wallets])
            await self.set_wallets_status(collected_wallets, 'free')

            task_request = SendRequest.model_validate(task.get('data'))
            transaction = await process_fastlane(task_request, target_wallet, self.wallet_storage)

            if not transaction.get('status'):
                logger.error('Error during process transaction')
                task['error'] = transaction.get('message')
                task['status'] = AggregatorStatus.FAILED.value
                self._update_task(task)
                await self.set_wallets_status([target_wallet], 'free')
                await asyncio.sleep(3)
                continue

            task['status'] = AggregatorStatus.COMPLETED.value
            task['tx_hash'] = transaction.get('tx_hash', None)
            self._update_task(task)
            await asyncio.sleep(3)

    def _iter_wallets(self, wallet_ids: list[str], status: Literal["free", "busy", "sync"]) -> bool:
        fresh_wallets = [wallet for wallet in self.wallet_storage.get_wallets() if wallet.id in wallet_ids]
        for wallet in fresh_wallets:
            if status == 'sync':
                if wallet.status != 'free':
                    return False
            if wallet.status != status:
                self.wallet_storage.set_wallet_status(wallet.id, status)
        return True

    async def set_wallets_status(self, wallets: list[Wallet], status: Literal["free", "busy", "sync"]):
        global_status = False
        wallet_ids = [wallet.id for wallet in wallets]
        while not global_status:
            global_status = self._iter_wallets(wallet_ids, status)
            if not global_status:
                await asyncio.sleep(1.5)

    async def update_wallets_balance(self, wallets: list[Wallet]):
        for wallet in wallets:
            await self.wallet_storage.update_wallet_balance(wallet.id, wallet.tonapi_key, wallet.mnemonic)

    def _update_task(self, target_task: dict):
        tasks = self._read_queue().get('tasks')
        for i in range(0, len(tasks)):
            if tasks[i].get('id') == target_task.get('id'):
                tasks[i] = target_task
        self._update_queue(tasks)

    def _read_queue(self) -> dict:
        with self.lock:
            with open(self.queue_file, 'r') as f:
                return json.load(f)

    def _update_queue(self, tasks: list[dict]):
        tasks = {
            'tasks': tasks
        }
        with self.lock:
            with open(self.queue_file, 'w') as f:
                json.dump(tasks, f, indent=2)

    def get_tasks(self) -> list[dict]:
        return self._read_queue().get('tasks')

    def add_task(self, msg: SendRequest, cost: float):
        task_data = msg.model_dump()
        logger.info(task_data)
        tasks: list[dict] = self._read_queue().get('tasks')
        task = {
            'id': str(uuid.uuid4()),
            'data': task_data,
            'cost': cost,
            'status': AggregatorStatus.PENDING.value,
            'error': "",
            'created_at': datetime.now().isoformat()
        }
        logger.info(f'current task: {task}')
        tasks.append(task)
        self._update_queue(tasks)
        return task.get('id')

    def del_task(self, task_id):
        tasks = self._read_queue().get('tasks')
        index = None
        for i in range(0, len(tasks)):
            if tasks[i].get('id') == task_id:
                index = i
                break
        del tasks[index]
