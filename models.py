from typing import Literal
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class SendRequest(BaseModel):
    currency: int
    username: str
    type: Literal["stars", "premium", "deleted_gift"]


@dataclass
class Cookies:
    stel_token: str
    stel_ssid: str
    stel_ton_token: str

    def to_dict(self):
        return {
            "stel_token": self.stel_token,
            "stel_ssid": self.stel_ssid,
            "stel_ton_token": self.stel_ton_token
      }

@dataclass
class Wallet:
    id: str

    address: str
    raw_address: str
    public_key: str

    mnemonic: list[str]
    tonapi_key: str
    hash: str
    cookies: Cookies

    status: Literal['free', 'busy', 'sync']
    balance: float

    created_at: datetime

    def to_dict(self) -> dict:
        return {
            "data": {
                "seed_phrase": " ".join(self.mnemonic),
                "address": self.address,
                "raw_address": self.raw_address,
                "public_key": self.public_key,
                "created_at": self.created_at.isoformat()
            },
            "connection": {
                "mnemonic": self.mnemonic,
                "tonapi_key": self.tonapi_key,
                "hash": self.hash,
                "cookies": {
                    "stel_token": self.cookies.stel_token,
                    "stel_ssid": self.cookies.stel_ssid,
                    "stel_ton_token": self.cookies.stel_ton_token
                }
            },
            "status": self.status,
            "balance": self.balance
        }

    @classmethod
    def from_dict(cls, wallet_id: str, wallet: dict):
        data = wallet.get('data')
        connection = wallet.get('connection')
        cookies = connection.get('cookies')
        return cls(
            id=wallet_id,
            address=data.get('address'),
            raw_address=data.get('raw_address'),
            public_key=data.get('public_key'),
            mnemonic=connection.get('mnemonic'),
            tonapi_key=connection.get('tonapi_key'),
            hash=connection.get('hash'),
            cookies=Cookies(
                stel_token=cookies.get('stel_token'),
                stel_ssid=cookies.get('stel_ssid'),
                stel_ton_token=cookies.get('stel_ton_token')
            ),
            status=wallet.get('status'),
            balance=wallet.get('balance'),
            created_at=datetime.fromisoformat(data.get('created_at'))
        )


class AggregatorStatus(str, Enum):
    PENDING = "pending"  # ждет выполнения
    PROCESSING = "processing"  # выполняется сейчас
    COMPLETED = "completed"  # выполнена
    FAILED = "failed"  # ошибка


