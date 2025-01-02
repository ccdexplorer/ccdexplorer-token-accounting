import datetime as dt

import aiohttp
import urllib3
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import CCD_BlockInfo, CCD_ModuleRef
from ccdexplorer_fundamentals.mongodb import (
    Collections,
    MongoDB,
    MongoMotor,
)
from ccdexplorer_fundamentals.tooter import Tooter
from pymongo.collection import Collection
from rich.console import Console

from env import COIN_API_KEY

# from .token_accounting import TokenAccounting as _token_accounting
from .token_accounting_v2 import TokenAccountingV2 as _token_accounting_v2
from .utils import Queue

urllib3.disable_warnings()
console = Console()


class Heartbeat(_token_accounting_v2):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
        net: str,
    ):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo
        self.net = net
        self.address_to_follow = None
        self.sending = False
        self.utilities: dict[Collections, Collection] = self.mongodb.utilities
        self.db: dict[Collections, Collection] = (
            self.mongodb.mainnet if self.net == "mainnet" else self.mongodb.testnet
        )
        self.motordb: dict[Collections, Collection] = (
            self.motormongo.testnet if net == "testnet" else self.motormongo.mainnet
        )
        self.finalized_block_infos_to_process: list[CCD_BlockInfo] = []
        self.special_purpose_block_infos_to_process: list[CCD_BlockInfo] = []

        self.existing_source_modules: dict[CCD_ModuleRef, set] = {}
        self.queues: dict[Collections, list] = {}
        for q in Queue:
            self.queues[q] = []

        # this gets set every time the log heartbeat last processed helper gets set
        # in block_loop we check if this value is < x min from now.
        # If so, we restart, as there's probably something wrong that a restart
        # can fix.
        self.internal_freqency_timer = dt.datetime.now().astimezone(tz=dt.timezone.utc)
        self.session = aiohttp.ClientSession()
        coin_api_headers = {
            "X-CoinAPI-Key": COIN_API_KEY,
        }
        self.coin_api_session = aiohttp.ClientSession(headers=coin_api_headers)

    def exit(self):
        self.session.close()
        self.coin_api_session.close()
