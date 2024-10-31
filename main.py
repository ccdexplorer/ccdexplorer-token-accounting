import atexit

import urllib3
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.mongodb import (
    MongoDB,
    MongoMotor,
)
from ccdexplorer_fundamentals.tooter import Tooter
from rich.console import Console
from scheduler.asyncio import Scheduler
from env import RUN_ON_NET
from heartbeat import Heartbeat
import datetime as dt

urllib3.disable_warnings()

console = Console()
grpcclient = GRPCClient()
tooter = Tooter()

mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)


def main():
    """ """
    console.log(f"{RUN_ON_NET=}")
    schedule = Scheduler()

    heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, RUN_ON_NET)
    atexit.register(heartbeat.exit)

    # loop = asyncio.get_event_loop()

    schedule.cyclic(dt.timedelta(seconds=1), heartbeat.update_token_accounting)
    schedule.cyclic(dt.timedelta(seconds=10), heartbeat.get_redo_token_addresses)
    schedule.cyclic(
        dt.timedelta(seconds=10), heartbeat.special_purpose_token_accounting
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as f:
        console.log("main error: ", f)
