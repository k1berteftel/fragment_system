import re
import json
import base64
import asyncio
from typing import Union, Dict, List, Optional, Any
import time
import aiohttp
import logging

from tonsdk.boc import Cell
import tonutils.client
import tonutils.wallet

from logic.wallets.manager import WalletStorage
from utils.transactions import check_transaction
from models import SendRequest, Wallet


logger = logging.getLogger(__name__)


FRAGMENT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded"
}


def strip_html_tags(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"&nbsp;?", " ", text)
    return text.strip()


def clean_and_filter(obj: Union[Dict, List, str, int, float, None]) -> Union[Dict, List, str, int, float, None]:
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            if k.endswith("_html"):
                continue
            clean_v = clean_and_filter(v)
            new[k] = clean_v
        return new
    if isinstance(obj, list):
        return [clean_and_filter(v) for v in obj]
    if isinstance(obj, str):
        return strip_html_tags(obj)
    return obj


class WalletManager:
    def __init__(self, api_key: str, mnemonic: List[str]):
        self.api_key = api_key
        self.mnemonic = mnemonic
        self.ton_client: Optional[tonutils.client.TonapiClient] = None
        self.wallet = None

    async def init_wallet(self):
        self.ton_client = tonutils.client.TonapiClient(api_key=self.api_key)
        self.wallet, _, _, _ = tonutils.wallet.WalletV4R2.from_mnemonic(
            self.ton_client, mnemonic=self.mnemonic
        )

    async def transfer(self, address: str, amount: float, comment: str) -> Dict[str, Any]:
        result = {
            "address": address,
            "amount": amount,
            "comment": comment,
            "success": False,
            "tx_hash": None,
            "error": None
        }
        try:
            tx_hash = await self.wallet.transfer(
                destination=address,
                amount=amount,
                body=comment
            )
            result["success"] = True
            result["tx_hash"] = tx_hash
        except Exception as e:
            result["error"] = str(e)
        return result

    async def close(self):
        if self.ton_client and hasattr(self.ton_client, "_session"):
            try:
                await self.ton_client._session.close()
                await self.ton_client.close_session()
            except Exception:
                ...


def decode_payload_b64(payload: str) -> str:
    try:
        payload += "=" * (-len(payload) % 4)
        cell = Cell.one_from_boc(base64.b64decode(payload))
        sl = cell.begin_parse()
        return sl.read_string().strip()
    except Exception as e:
        return f"decode_error: {e}"


def decode_payload_b64_premium(payload: str) -> str:
    try:
        payload += "=" * (-len(payload) % 4)
        raw_bytes = base64.b64decode(payload)
        decoded = raw_bytes.decode('utf-8', errors='ignore')
        filtered = ''.join(ch for ch in decoded if 32 <= ord(ch) <= 126 or ch in '\r\n')
        filtered = re.sub(r'\r\n?', '\n', filtered)
        filtered = re.sub(r'[ ]*\n+', '\n\n', filtered).strip()
        idx = filtered.find("Telegram Premium")
        if idx != -1:
            filtered = filtered[idx:]
        return filtered
    except Exception as e:
        return f"decode_error: {e}"


async def buy_stars_logic(login: str, quantity: int, mnemonic, tonapi_key, hash, cookies) -> Dict[str, Any]:
    wm = WalletManager(tonapi_key, mnemonic)
    await wm.init_wallet()
    results: Dict[str, Any] = {}
    async with aiohttp.ClientSession(cookies=cookies, headers=FRAGMENT_HEADERS) as session:
        steps = [
            ("updateStarsBuyState", {"mode": "new", "lv": "false", "dh": "1", "method": "updateStarsBuyState"}),
            ("searchStarsRecipient", {"query": login, "quantity": str(quantity), "method": "searchStarsRecipient"}),
            ("updateStarsPrices", {"stars": "", "quantity": str(quantity), "method": "updateStarsPrices"}),
            ("initBuyStarsRequest", {"recipient": None, "quantity": str(quantity), "method": "initBuyStarsRequest"}),
        ]
        for name, data in steps:
            if name == "initBuyStarsRequest":
                recipient = results["searchStarsRecipient"].get("found", {}).get("recipient")
                if not recipient:
                    break
                data["recipient"] = recipient
            async with session.post(f"https://fragment.com/api?hash={hash}", data=data) as resp:
                raw = await resp.json()
            results[name] = clean_and_filter(raw)
            if name == "searchStarsRecipient" and "found" not in raw:
                await wm.close()
                return clean_and_filter(results)
            if name == "initBuyStarsRequest" and not raw.get("req_id"):
                await wm.close()
                return clean_and_filter(results)
        req_id = results["initBuyStarsRequest"]["req_id"]
        account = ""
        device = {
            "platform": "browser",
            "appName": "telegram-wallet",
            "appVersion": "1",
            "maxProtocolVersion": 2,
            "features": ["SendTransaction", {"name": "SendTransaction", "maxMessages": 4, "extraCurrencySupported": True}]
        }
        data5 = {
            "account": json.dumps(account),
            "device": json.dumps(device),
            "transaction": "1",
            "id": req_id,
            "show_sender": str(0),
            "method": "getBuyStarsLink"
        }
        async with session.post(f"https://fragment.com/api?hash={hash}", data=data5) as resp5:
            raw5 = await resp5.json()
        results["getBuyStarsLink"] = clean_and_filter(raw5)
        if not raw5.get("ok") or "transaction" not in raw5:
            await wm.close()
            return clean_and_filter(results)
        transfers = []
        for msg in raw5["transaction"].get("messages", []):
            addr = msg["address"]
            amount_ton = int(msg["amount"]) / 1e9
            raw_payload = msg.get("payload", "")
            decoded = decode_payload_b64(raw_payload)
            transfers.append(await wm.transfer(addr, amount_ton, decoded))
        results["transfers"] = transfers
    await wm.close()
    return clean_and_filter(results)


async def buy_premium_logic(login: str, months: int, mnemonic, tonapi_key, hash, cookies) -> Dict[str, Any]:
    wm = WalletManager(tonapi_key, mnemonic)
    await wm.init_wallet()
    results: Dict[str, Any] = {}
    async with aiohttp.ClientSession(cookies=cookies, headers=FRAGMENT_HEADERS) as session:
        steps = [
            ("updatePremiumState", {"mode": "new", "lv": "false", "dh": "1", "method": "updatePremiumState"}),
            ("searchPremiumGiftRecipient", {"query": login, "method": "searchPremiumGiftRecipient"}),
            ("initGiftPremiumRequest", {"recipient": None, "months": str(months), "method": "initGiftPremiumRequest"}),
        ]
        for name, data in steps:
            if name == "initGiftPremiumRequest":
                recipient = results["searchPremiumGiftRecipient"].get("found", {}).get("recipient")
                if not recipient:
                    break
                data["recipient"] = recipient
            async with session.post(f"https://fragment.com/api?hash={hash}", data=data) as resp:
                raw = await resp.json()
            results[name] = clean_and_filter(raw)
            if name == "searchPremiumGiftRecipient" and "found" not in raw:
                await wm.close()
                return clean_and_filter(results)
            if name == "initGiftPremiumRequest" and not raw.get("req_id"):
                await wm.close()
                return clean_and_filter(results)
        req_id = results["initGiftPremiumRequest"]["req_id"]
        account = ""
        device = {
            "platform": "browser",
            "appName": "telegram-wallet",
            "appVersion": "1",
            "maxProtocolVersion": 2,
            "features": ["SendTransaction", {"name": "SendTransaction", "maxMessages": 4, "extraCurrencySupported": True}]
        }
        data4 = {
            "account": json.dumps(account),
            "device": json.dumps(device),
            "transaction": "1",
            "id": req_id,
            "show_sender": str(0),
            "method": "getGiftPremiumLink"
        }
        async with session.post(f"https://fragment.com/api?hash={hash}", data=data4) as resp4:
            raw4 = await resp4.json()
        results["getGiftPremiumLink"] = clean_and_filter(raw4)
        if not raw4.get("ok") or "transaction" not in raw4:
            await wm.close()
            return clean_and_filter(results)
        transfers = []
        for msg in raw4["transaction"].get("messages", []):
            addr = msg["address"]
            amount_ton = int(msg["amount"]) / 1e9
            raw_payload = msg.get("payload", "")
            decoded = decode_payload_b64_premium(raw_payload)
            transfers.append(await wm.transfer(addr, amount_ton, decoded))
        results["transfers"] = transfers
    await wm.close()
    return clean_and_filter(results)


async def send_request(msg: SendRequest, wallet: Wallet) -> tuple[bool, str]:
    logger.info(f'Send buy-request for wallet: "{wallet.id}". Type: {msg.type}')
    mnemonic, tonapi_key, hash, cookies = wallet.mnemonic, wallet.tonapi_key, wallet.hash, wallet.cookies
    try:
        if msg.type == 'stars':
            data = await buy_stars_logic(msg.username, msg.currency, mnemonic, tonapi_key, hash, cookies.to_dict())
        else:
            data = await buy_premium_logic(msg.username, msg.currency, mnemonic, tonapi_key, hash, cookies.to_dict())
    except Exception as err:
        logger.error(f'Error during buy logic: {err}')
        raise
    logger.info(f'Received data: {data}')
    transfers = data.get('transfers')
    if not transfers:
        logger.error('No transfers data in response')
        raise Exception
    tx_hash = transfers[0].get('tx_hash')
    if not tx_hash:
        logger.error('No tx_hash data in transfers')
        raise Exception
    logger.info('Buy-logic data has all needed args (success)')
    try:
        logger.info("Start check buy-logic tx_hash...")
        status = await check_transaction(tx_hash, tonapi_key)  # должен быть следующий вызов
    except Exception:
        return False, tx_hash
    return status, tx_hash


async def test():
    wallet_storage = WalletStorage()
    wallet = wallet_storage.get_wallets()[0]

    mnemonic, tonapi_key, hash, cookies = wallet.mnemonic, wallet.tonapi_key, wallet.hash, wallet.cookies
    try:
        data = await buy_stars_logic("@farion", 50, mnemonic, tonapi_key, hash, cookies.to_dict())
    except Exception as err:
        logger.error(f'Error during buy logic: {err}')
        raise
    print(data)


asyncio.run(test())