from __future__ import annotations

import logging
import asyncio
import httpx
import json
import time
from typing import TYPE_CHECKING, get_args

from sys_types import (
    ConfigurationError,
    FragmentAPIError,
    UnexpectedError,
    UserNotFoundError,
    VerificationError,
    FragmentBaseError,

    DEVICE_FINGERPRINT,
    EVM_PAYMENT_METHODS,
    STARS_PAGE,
    PREMIUM_GIFT_PAGE,
    TON_PAYMENT_METHODS,
    DEFAULT_TIMEOUT,

    PremiumResult,
    StarsResult,
    Wallet,
    EvmPaymentResult,
    SendRequest
)
from logic.wallets.manager import WalletStorage

from utils.transactions import check_transaction
from utils.http import build_headers, fetch_fragment_hash, post_FragmentAPI
from utils.evm import fetch_evm_invoice
from utils.api import confirm_request #, call
from utils.wallet import build_account_info, execute_transaction


logger = logging.getLogger(__name__)


async def purchase_stars(
    wallet: Wallet,
    username: str,
    amount: int,
    show_sender: bool = False,
    payment_method: str = "ton",
) -> StarsResult | EvmPaymentResult:
    '''
    Send Telegram Stars to a user.

    Args:
        payment_method: One of "ton", "usdt_ton", "usdt_eth", "usdt_pol",
            "usdc_eth", "usdc_base", "usdc_pol".

    Returns:
        StarsResult (for TON-based methods) or EvmPaymentResult
        (for EVM methods — caller must complete payment manually).
    '''
    if not isinstance(amount, int) or not (50 <= amount <= 1_000_000):
        raise ConfigurationError(ConfigurationError.INVALID_STARS_AMOUNT)

    try:
        headers = build_headers(STARS_PAGE)

        async with httpx.AsyncClient(
            cookies=wallet.cookies.to_dict(),
            timeout=DEFAULT_TIMEOUT,
        ) as session:
            # fragment_hash = await fetch_fragment_hash(
            #     client.cookies,
            #     headers,
            #     STARS_PAGE,
            #     DEFAULT_TIMEOUT,
            # )

            result = await post_FragmentAPI(
                session,
                wallet.hash,
                headers,
                {
                    "method": "searchStarsRecipient",
                    "query": username,
                    "quantity": "",
                },
            )
            recipient = result.get("found", {}).get("recipient")
            if not recipient:
                raise UserNotFoundError(
                    UserNotFoundError.NOT_FOUND.format(username=username),
                )

            result = await post_FragmentAPI(
                session,
                wallet.hash,
                headers,
                {
                    "method": "initBuyStarsRequest",
                    "recipient": recipient,
                    "quantity": str(amount),
                    "payment_method": payment_method,
                },
            )
            if result.get("error"):
                raise FragmentAPIError(result["error"])

            req_id = result.get("req_id")
            if not req_id:
                raise FragmentAPIError(
                    FragmentAPIError.NO_REQUEST_ID.format(
                        context="Stars purchase",
                    )
                )

            account = await build_account_info(wallet)
            transaction = await post_FragmentAPI(
                session,
                wallet.hash,
                headers,
                {
                    "method": "getBuyStarsLink",
                    "account": json.dumps(account),
                    "device": DEVICE_FINGERPRINT,
                    "transaction": 1,
                    "id": req_id,
                    "show_sender": int(show_sender),
                },
            )
            if transaction.get("need_verify"):
                raise VerificationError(VerificationError.KYC_REQUIRED)

        if payment_method in EVM_PAYMENT_METHODS or transaction.get("evm"):
            invoice = await fetch_evm_invoice(
                cookies=wallet.cookies.to_dict(),
                page_path="/stars/buy",
                recipient=recipient,
                payment_method=payment_method,
                quantity=amount,
                timeout=DEFAULT_TIMEOUT,
            )
            return EvmPaymentResult(
                item_kind="stars",
                target=username,
                amount=amount,
                payment_method=payment_method,
                invoice=invoice,
            )

        if payment_method not in TON_PAYMENT_METHODS:
            raise FragmentAPIError(
                f"Unsupported payment_method flow: {payment_method}"
            )

        print(f'result before execute transaction: {transaction}')
        print(transaction['transaction']['messages'][0])
        print(transaction['transaction']['messages'][0]['amount'])
        print(float(transaction['transaction']['messages'][0].get('amount')) / 1_000_000_000)
        tx_result = await execute_transaction(wallet, transaction)

        if tx_result.boc and req_id:
            try:
                await confirm_request(
                    wallet,
                    req_id,
                    tx_result.boc,
                    referer="stars/buy",
                )
            except Exception:
                pass

        return StarsResult(
            transaction_id=tx_result.tx_hash,
            username=username,
            amount=amount,
            payment_method=payment_method,
        )

    except FragmentBaseError:
        raise
    except Exception as exc:
        raise UnexpectedError(
            UnexpectedError.UNEXPECTED.format(exc=exc),
        ) from exc


async def purchase_premium(
    wallet: Wallet,
    username: str,
    months: int,
    show_sender: bool = False,
    payment_method: str = "ton",
) -> PremiumResult | EvmPaymentResult:
    '''
    Gift Telegram Premium to a user.

    Supports TON, USDT (TON), and EVM-based payments
    (USDT/USDC on ETH/BASE/POL).
    '''
    if months not in (3, 6, 12):
        raise ConfigurationError(ConfigurationError.INVALID_MONTHS)

    try:
        headers = build_headers(PREMIUM_GIFT_PAGE)

        async with httpx.AsyncClient(
            cookies=wallet.cookies.to_dict(),
            timeout=DEFAULT_TIMEOUT,
        ) as session:
            # fragment_hash = await fetch_fragment_hash(
            #     wallet.cookies.to_dict(),
            #     headers,
            #     PREMIUM_GIFT_PAGE,
            #     DEFAULT_TIMEOUT,
            # )

            result = await post_FragmentAPI(
                session,
                wallet.hash,
                headers,
                {
                    "method": "searchPremiumGiftRecipient",
                    "query": username,
                    "months": months,
                },
            )
            recipient = result.get("found", {}).get("recipient")
            if not recipient:
                raise UserNotFoundError(
                    UserNotFoundError.NOT_FOUND.format(username=username),
                )

            await post_FragmentAPI(
                session,
                wallet.hash,
                headers,
                {
                    "method": "updatePremiumState",
                    "mode": "new",
                    "lv": "false",
                    "dh": str(int(time.time())),
                },
            )

            result = await post_FragmentAPI(
                session,
                wallet.hash,
                headers,
                {
                    "method": "initGiftPremiumRequest",
                    "recipient": recipient,
                    "months": str(months),
                    "payment_method": payment_method,
                },
            )
            if result.get("error"):
                raise FragmentAPIError(result["error"])

            req_id = result.get("req_id")
            if not req_id:
                raise FragmentAPIError(
                    FragmentAPIError.NO_REQUEST_ID.format(
                        context="Premium purchase",
                    )
                )

            account = await build_account_info(wallet)
            transaction = await post_FragmentAPI(
                session,
                wallet.hash,
                headers,
                {
                    "method": "getGiftPremiumLink",
                    "account": json.dumps(account),
                    "device": DEVICE_FINGERPRINT,
                    "transaction": 1,
                    "id": req_id,
                    "show_sender": int(show_sender),
                },
            )
            if transaction.get("need_verify"):
                raise VerificationError(VerificationError.KYC_REQUIRED)

        if payment_method in EVM_PAYMENT_METHODS or transaction.get("evm"):
            invoice = await fetch_evm_invoice(
                cookies=wallet.cookies.to_dict(),
                page_path="/premium/gift",
                recipient=recipient,
                payment_method=payment_method,
                months=months,
                timeout=DEFAULT_TIMEOUT,
            )
            return EvmPaymentResult(
                item_kind="premium",
                target=username,
                amount=months,
                payment_method=payment_method,
                invoice=invoice,
            )

        if payment_method not in TON_PAYMENT_METHODS:
            raise FragmentAPIError(
                f"Unsupported payment_method flow: {payment_method}"
            )

        tx_result = await execute_transaction(wallet, transaction)

        if tx_result.boc and req_id:
            try:
                await confirm_request(
                    wallet,
                    req_id,
                    tx_result.boc,
                    referer="premium/gift",
                )
            except Exception:
                pass

        return PremiumResult(
            transaction_id=tx_result.tx_hash,
            username=username,
            amount=months,
            payment_method=payment_method,
        )

    except FragmentBaseError:
        raise
    except Exception as exc:
        raise UnexpectedError(
            UnexpectedError.UNEXPECTED.format(exc=exc),
        ) from exc


async def send_request(msg: SendRequest, wallet: Wallet) -> tuple[bool, str]:
    logger.info('Start send request')
    try:
        if msg.type == 'stars':
            result = await purchase_stars(wallet, msg.username, msg.currency)
            if isinstance(result, StarsResult):
                tx_hash = result.transaction_id
            else:
                return False, ''
        else:
            result = await purchase_premium(wallet, msg.username, msg.currency)
            if isinstance(result, PremiumResult):
                tx_hash = result.transaction_id
            else:
                return False, ''
    except Exception as err:
        logger.error(f'Error during execute purchase operation: {err}')
        return False, ''

    logger.info('Success execute purchase, start checking transaction in wallet...')
    try:
        status = await check_transaction(tx_hash, wallet.tonapi_key)
    except Exception as err:
        logger.error(f'Critical error during check transaction: {err}')
        status = False

    logger.info('Transaction has checked successfully')
    return status, tx_hash


async def test(currency: int, username: str, msg_type: str):
    wallet_storage = WalletStorage()
    wallet = wallet_storage.get_wallets()[1]
    await wallet_storage.update_wallet_balance(wallet.id, wallet.tonapi_key, wallet.mnemonic)

    print('start test')
    logger.info('Start send request')
    try:
        if msg_type == 'stars':
            result = await purchase_stars(wallet, username, currency)
            if isinstance(result, StarsResult):
                tx_hash = result.transaction_id
            else:
                return False, ''
        else:
            result = await purchase_premium(wallet, username, currency)
            if isinstance(result, PremiumResult):
                tx_hash = result.transaction_id
            else:
                return False, ''
    except Exception as err:
        logger.error(f'Error during execute purchase operation: {err}')
        return False, ''

    print(tx_hash)

    print('Success execute purchase, start checking transaction in wallet...')
    logger.info('Success execute purchase, start checking transaction in wallet...')
    try:
        status = await check_transaction(tx_hash, wallet.tonapi_key)
    except Exception as err:
        logger.error(f'Critical error during check transaction: {err}')
        print(f'Critical error during check transaction: {err}')
        status = False

    logger.info('Transaction has checked successfully')
    print(status)
    return status, tx_hash


# print(asyncio.run(test(50, '@Leggit_dev', 'stars')))

