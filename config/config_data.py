from dataclasses import dataclass

from environs import Env

'''
    При необходимости конфиг базы данных или других сторонних сервисов
'''


@dataclass
class Fragment:
    api_key: str


@dataclass
class Wallet:
    seed_phrase: list[str]


@dataclass
class UserBot:
    api_id: int
    api_hash: str


@dataclass
class Config:
    fragment: Fragment
    wallet: Wallet
    user_bot: UserBot


def load_config(path: str | None = None) -> Config:
    env: Env = Env()
    env.read_env(path)

    return Config(
        fragment=Fragment(
            api_key=env('fragment_api_key')
        ),
        wallet=Wallet(
            seed_phrase=env('seed_phrase').split(' ')
        ),
        user_bot=UserBot(
            api_id=int(env('api_id')),
            api_hash=env('api_hash')
        )
    )
