import logging
import asyncio

from logic.wallets.manager import WalletStorage
from logic.purchase import send_request
from sys_types import SendRequest, Wallet


logger = logging.getLogger(__name__)


async def delay_set_wallet_status(wallet_id: str, wallet_storage: WalletStorage):
    await asyncio.sleep(2)
    wallet_storage.set_wallet_status(wallet_id, 'free')
    await asyncio.sleep(13)
    wallet_storage.set_cooldown(wallet_id, None)


async def process_fastlane(msg: SendRequest, wallet: Wallet, wallet_storage: WalletStorage):
    logger.info(f'Get fastlane message: {msg.username}|{msg.currency}|{msg.type}')
    wallet_storage.set_wallet_status(wallet.id, 'busy')

    counter = 0
    error_message = ''
    logger.info(f'Start send request')
    while True:
        logger.info(f'Attempt: {counter}')
        if counter >= 2:
            logger.warning('Failed to process fastlane. Attempts are over')
            wallet_storage.set_wallet_status(wallet.id, 'free')
            wallet_storage.set_cooldown(wallet.id, 15)
            return {
                'status': False,
                'message': error_message
            }
        try:
            status, tx_hash = await send_request(msg, wallet)
            break
        except Exception as err:
            logger.error(f'Attempt: {counter}. Error: {err}')
            error_message = str(err)
            counter += 1
            await asyncio.sleep(1.5)
    logger.info(f'Send request executed successfully. Status: {status}')
    await wallet_storage.update_wallet_balance(wallet.id, wallet.tonapi_key, wallet.mnemonic)
    if status:
        wallet_storage.set_cooldown(wallet.id, 15)
    asyncio.create_task(delay_set_wallet_status(wallet.id, wallet_storage))
    return {
        'status': status,
        'tx_hash': tx_hash
    }
