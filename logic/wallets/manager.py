import asyncio
import json
import threading
import aiohttp
import logging
from typing import Literal

import tonutils.client
import tonutils.wallet

from sys_types import Wallet


logger = logging.getLogger(__name__)


class WalletStorage:
    def __init__(self, path: str = 'wallets.json'):
        self._path = path
        self.lock = threading.RLock()

    def _read_file(self) -> dict:
        with self.lock:
            with open(self._path, 'r', encoding='utf-8') as f:
                wallets = json.loads(f.read())
            return wallets

    def _update_file(self, wallets: dict):
        with self.lock:
            with open(self._path, 'w+', encoding='utf-8') as f:
                json.dump(wallets, f, indent=4)

    async def _get_wallet_balance(self, tonapi_key: str, mnemonic: list[str]) -> float | None:
        counter = 0
        while counter < 3:
            try:
                ton_client = tonutils.client.TonapiClient(api_key=tonapi_key)
                ton_wallet, _, _, _ = tonutils.wallet.WalletV4R2.from_mnemonic(
                    ton_client,
                    mnemonic=mnemonic
                )
                balance = await ton_wallet.balance()
                return balance
            except Exception as err:
                logger.error(f'Error during update balance: {err}')
                counter += 1
                logger.info(f'Counter: {1}')
                await asyncio.sleep(1)
        return None

    def get_wallets(self) -> list[Wallet]:
        wallets = self._read_file()
        wallets = [Wallet.from_dict(wallet_id, wallet) for wallet_id, wallet in wallets.items()]
        return wallets

    async def update_wallet_balance(self, wallet_id: str, tonapi_key: str, mnemonic: list[str]):
        balance = await self._get_wallet_balance(tonapi_key, mnemonic)
        if balance is None:
            return
        wallets = self._read_file()
        wallets[wallet_id]['balance'] = balance
        self._update_file(wallets)

    def set_wallet_status(self, wallet_id: str, status: Literal["free", "busy", "sync"]):
        wallets = self._read_file()
        wallets[wallet_id]['status'] = status
        self._update_file(wallets)

