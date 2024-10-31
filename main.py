import atexit
import asyncio
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


async def main():
    """ """
    console.log(f"{RUN_ON_NET=}")
    loop = asyncio.get_running_loop()
    schedule = Scheduler(loop=loop)

    heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, RUN_ON_NET)
    atexit.register(heartbeat.exit)

    # loop = asyncio.get_event_loop()

    schedule.cyclic(dt.timedelta(seconds=1), heartbeat.update_token_accounting)
    schedule.cyclic(dt.timedelta(seconds=10), heartbeat.get_redo_token_addresses)
    schedule.cyclic(
        dt.timedelta(seconds=10), heartbeat.special_purpose_token_accounting
    )
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
