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
from env import RUN_ON_NET, MQTT_PASSWORD, MQTT_QOS, MQTT_SERVER, MQTT_USER
from heartbeat import Heartbeat
import datetime as dt
import paho.mqtt.client as mqtt

urllib3.disable_warnings()

console = Console()
grpcclient = GRPCClient()
tooter = Tooter()

mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    console.log(f"MQTT connected with result code: {reason_code}")


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    if reason_code_list[0].is_failure:
        console.log(f"Broker rejected you subscription: {reason_code_list[0]}")
    else:
        console.log(f"Broker granted the following QoS: {reason_code_list[0].value}")


mqttc = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    f"mqtt-{RUN_ON_NET}-token-accounting",
)

mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

mqttc.username_pw_set(MQTT_USER, MQTT_PASSWORD)
mqttc.connect(MQTT_SERVER, 1883, 10)
mqttc.subscribe("ccdexplorer/services/accounting/restart", qos=MQTT_QOS)
mqttc.loop_start()


async def main():
    """ """
    console.log(f"{RUN_ON_NET=}")
    loop = asyncio.get_running_loop()
    schedule = Scheduler(loop=loop)
    # schedule = Scheduler()

    heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, mqttc, RUN_ON_NET)
    atexit.register(heartbeat.exit)

    # loop = asyncio.get_event_loop()

    schedule.cyclic(dt.timedelta(seconds=1), heartbeat.update_token_accounting_v2)

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
