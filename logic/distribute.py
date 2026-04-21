import asyncio
import logging
import aiohttp
from typing import List, Dict, Optional
import tonutils.client
import tonutils.wallet

from utils.transactions import check_transaction
from logic.wallets.manager import WalletStorage
from config.config_data import Config, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config: Config = load_config()


def calculate_distribution(wallets: list[dict], total_amount) -> List[Dict]:
    """
    Рассчитывает распределение средств между кошельками

    Returns:
        список словарей с добавленными ключами:
        - 'balance': исходный баланс
        - 'top_up_amount': сумма для пополнения (без комиссии)
        - 'total_cost': общая стоимость (сумма пополнения + комиссия)
        - 'fee': размер комиссии
    """
    TX_FEE = 0.01  # комиссия за одну транзакцию пополнения в TON

    n = len(wallets)
    balances = [w['balance'] for w in wallets]

    indexed_wallets = sorted([(balance, i) for i, balance in enumerate(balances)])

    top_up_amounts = [0] * n
    remaining = total_amount

    for level in range(n):
        current_balance, idx = indexed_wallets[level]
        count = level + 1

        if level + 1 < n:
            next_balance = indexed_wallets[level + 1][0]
            needed = (next_balance - current_balance) * count

            if needed <= remaining:
                for j in range(level + 1):
                    orig_idx = indexed_wallets[j][1]
                    add = next_balance - indexed_wallets[j][0]
                    top_up_amounts[orig_idx] += add
                remaining -= needed
                for j in range(level + 1):
                    indexed_wallets[j] = (next_balance, indexed_wallets[j][1])
            else:
                add_per_wallet = remaining / count
                for j in range(level + 1):
                    orig_idx = indexed_wallets[j][1]
                    top_up_amounts[orig_idx] += add_per_wallet
                remaining = 0
                break
        else:
            add_per_wallet = remaining / count
            for j in range(level + 1):
                orig_idx = indexed_wallets[j][1]
                top_up_amounts[orig_idx] += add_per_wallet
            remaining = 0
            break

    # Формируем результат с учетом комиссий
    result = []
    for i, wallet in enumerate(wallets):
        top_up = top_up_amounts[i]
        fee = TX_FEE if top_up > 0 else 0

        result.append({
            **wallet,  # сохраняем все исходные ключи
            'top_up_amount': round(top_up, 6),  # сумма для пополнения (без комиссии)
            'fee': fee,  # размер комиссии
            'total_cost': round(top_up + fee, 6) if top_up > 0 else 0  # общая стоимость
        })

    return result


async def transfer_funds(
        client: tonutils.client.TonapiClient,
        mnemonic: list,
        address: str,
        amount: float,
        tonapi_key: str,
        retry_count: int = 0,
        max_retries: int = 3
) -> bool:
    if amount <= 0:
        logger.info(f'Пропуск перевода на {address}: сумма {amount} <= 0')
        return True

    try:
        # Создаем кошелек отправителя
        wallet, _, _, _ = tonutils.wallet.WalletV4R2.from_mnemonic(
            client,
            mnemonic=mnemonic
        )

        # Получаем актуальный seqno
        seqno = await wallet.get_seqno(client, wallet.address)

        # Отправляем транзакцию и получаем хеш
        tx_hash = await wallet.transfer(
            destination=address,
            amount=amount,
            comment='Distribution topup'
        )

        logger.info(f'Транзакция {amount} TON на {address} отправлена, хеш: {tx_hash}')
        await asyncio.sleep(3)

        logger.info(f'Проверка транзакции {tx_hash[:8]}...')
        is_confirmed = await check_transaction(
            tx_hash=tx_hash,
            TONAPI_KEY=tonapi_key,
            max_attempts=10,
            base_delay=2.0
        )

        if is_confirmed:
            logger.info(f'✅ Транзакция {tx_hash[:8]} успешно подтверждена для {address}')
            return True
        else:
            logger.error(f'❌ Транзакция {tx_hash[:8]} не подтвердилась для {address}')

            if retry_count < max_retries:
                logger.info(f'Повторная попытка {retry_count + 1}/{max_retries}...')
                await asyncio.sleep(5)
                return await transfer_funds(
                    client, mnemonic, address, amount, tonapi_key,
                    retry_count + 1, max_retries
                )
            return False

    except Exception as e:
        error_msg = str(e)

        # Обработка ошибок seqno
        if "Duplicate msg_seqno" in error_msg or "Too old seqno" in error_msg:
            if retry_count < max_retries:
                logger.error(f'Ошибка seqno для {address}: {error_msg}')
                logger.info(f'Попытка {retry_count + 1}/{max_retries}, ждем 5 секунд...')
                await asyncio.sleep(5)
                return await transfer_funds(
                    client, mnemonic, address, amount, tonapi_key,
                    retry_count + 1, max_retries
                )
            else:
                logger.error(f'Не удалось отправить на {address} после {max_retries} попыток')
                return False

        elif "Connection reset" in error_msg:
            logger.error(f'Ошибка соединения для {address}, повтор через 5 секунд...')
            await asyncio.sleep(5)
            if retry_count < max_retries:
                return await transfer_funds(
                    client, mnemonic, address, amount, tonapi_key,
                    retry_count + 1, max_retries
                )
            return False
        else:
            logger.error(f'Неизвестная ошибка: {error_msg}')
            return False


async def distribute_endpoint(wallet_storage: WalletStorage):
    """
    Распределение средств между кошельками с проверкой транзакций
    """
    storage_wallets = wallet_storage.get_wallets()

    # Блокируем кошельки
    blocked = 0
    while True:
        for wallet in wallet_storage.get_wallets():
            if wallet.status == 'free':
                wallet_storage.set_wallet_status(wallet.id, 'sync')
                blocked += 1

        if blocked == len(storage_wallets):
            break
        await asyncio.sleep(0.5)


    wallets = []
    for wallet in storage_wallets:
        ton_client = tonutils.client.TonapiClient(api_key=wallet.tonapi_key)
        ton_wallet, _, _, _ = tonutils.wallet.WalletV4R2.from_mnemonic(
            ton_client,
            mnemonic=wallet.mnemonic
        )

        balance = await ton_wallet.balance()
        logger.info(f'{wallet.id} balance: {balance}')
        wallets.append({
            'id': wallet.id,
            'ton_client': ton_client,
            'balance': balance if balance else 0.0,
            'address': ton_wallet.address.to_str(),
            'tonapi_key': wallet.tonapi_key
        })

    main_client = wallets[0]['ton_client']
    main_tonapi_key = wallets[0]['tonapi_key']

    main_wallet_temp, _, _, _ = tonutils.wallet.WalletV4R2.from_mnemonic(
        main_client,
        mnemonic=config.wallet.seed_phrase
    )

    initial_seqno = await main_wallet_temp.get_seqno(main_client, main_wallet_temp.address)

    total_amount = await main_wallet_temp.balance()
    logger.info(f'Общая сумма для распределения: {total_amount} TON')

    distribution = calculate_distribution(wallets, total_amount)
    distribution.sort(key=lambda x: x.get('top_up_amount', 0), reverse=True)

    logger.info('НАЧАЛО РАСПРЕДЕЛЕНИЯ СРЕДСТВ')

    successful = 0
    failed = 0

    for i, wallet_info in enumerate(distribution):
        amount = wallet_info.get('top_up_amount', 0)

        if amount > 0:
            logger.info(f'\n📤 {i + 1}/{len(distribution)}: Отправка {amount} TON на кошелек {wallet_info["id"]}')

            success = await transfer_funds(
                main_client,
                config.wallet.seed_phrase,
                wallet_info['address'],
                amount,
                main_tonapi_key
            )

            if success:
                successful += 1
                logger.info(f'   ✅ УСПЕШНО')
            else:
                failed += 1
                logger.warning(f'   ❌ НЕУДАЧА')

        if i < len(distribution) - 1:
            await asyncio.sleep(2)

    logger.info(f'Distribution completed: {successful} success, {failed} failed')

    # Обновляем балансы и разблокируем кошельки
    for wallet in storage_wallets:
        try:
            await wallet_storage.update_wallet_balance(wallet.id, wallet.tonapi_key, wallet.mnemonic)
            wallet_storage.set_wallet_status(wallet.id, 'free')
        except Exception as e:
            logger.error(f'Ошибка при обновлении кошелька {wallet.id}: {e}')

    return failed == 0


# async def test():
#     """
#     Тестовая функция
#     """
#     try:
#         wallet_storage = WalletStorage()
#         result = await distribute_endpoint(wallet_storage)
#
#         print('\n' + '=' * 60)
#         if result:
#             print('🎉 ВСЕ ТРАНЗАКЦИИ УСПЕШНО ВЫПОЛНЕНЫ!')
#         else:
#             print('⚠️  НЕКОТОРЫЕ ТРАНЗАКЦИИ НЕ ПОДТВЕРДИЛИСЬ')
#             print('Рекомендуется проверить логи и неудачные транзакции')
#         print('=' * 60)
#
#     except Exception as e:
#         logger.error(f"Ошибка: {e}")
#         print(f"❌ Критическая ошибка: {e}")
#
#
# if __name__ == "__main__":
#     asyncio.run(test())