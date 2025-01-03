import asyncio
import json

import paho.mqtt.client as mqtt
from ccdexplorer_fundamentals.cis import (
    MongoTypeLoggedEventV2,
    MongoTypeTokenAddress,
    MongoTypeTokenAddressV2,
    MongoTypeTokenForAddress,
    MongoTypeTokenLink,
)
from ccdexplorer_fundamentals.mongodb import (
    Collections,
)
from pymongo import ASCENDING, ReplaceOne
from pymongo.collection import Collection
from rich.console import Console

from env import MQTT_QOS

from .utils import Utils

console = Console()


########### Token Accounting V3
class TokenAccountingV2(Utils):
    async def update_token_accounting_v2(self):
        """
        This method takes logged events and processes them for
        token accounting. Note that token accounting only processes events with
        tag 255, 254, 253 and 251, which are transfer, mint, burn and metadata.
        The starting point is reading the helper document
        'token_accounting_last_processed_block', if that is either
        not there or set to -1, all token_addresses (and associated
        token_accounts) will be reset.
        """
        self.db: dict[Collections, Collection]
        self.mqtt: mqtt.Client
        # try:
        while self.sending:
            await asyncio.sleep(0.3)
            print("waiting for sending to finish")
        # Read token_accounting_last_processed_block
        result = self.db[Collections.helpers].find_one(
            {"_id": "token_accounting_last_processed_block_v3"}
        )
        # If it's not set, set to -1, which leads to resetting
        # all token addresses and accounts, basically starting
        # over with token accounting.
        if result:
            token_accounting_last_processed_block = result["height"]
        else:
            token_accounting_last_processed_block = -1

        # Query the logged events collection for all logged events
        # after 'token_accounting_last_processed_block'.
        # Logged events are ordered by block_height, then by
        # transaction index (tx_index) and finally by event index
        # (ordering).
        pipeline = [
            {"$match": {"event_info.standard": "CIS-2"}},
            {
                "$match": {
                    "tx_info.block_height": {
                        "$gt": token_accounting_last_processed_block
                    }
                }
            },
            {
                "$sort": {
                    "tx_info.block_height": ASCENDING,
                }
            },
            {"$limit": 1_000},
        ]
        result: list[MongoTypeLoggedEventV2] = [
            MongoTypeLoggedEventV2(**x)
            for x in self.db[Collections.tokens_logged_events_v2].aggregate(pipeline)
        ]

        # Only continue if there are logged events to process...
        token_addresses_to_update = {}
        if len(result) > 0:
            # When all logged events are processed,
            # 'token_accounting_last_processed_block' is set to
            # 'token_accounting_last_processed_block_when_done'
            # such that next iteration, we will not be re-processing
            # logged events we already have processed.
            token_accounting_last_processed_block_when_done = max(
                [x.tx_info.block_height for x in result]
            )

            # Dict 'events_by_token_address' is keyed on token_address
            # and contains an ordered list of logged events related to
            # this token_address.
            events_by_token_address: dict[str, list] = {}
            for log in result:
                events_by_token_address[log.event_info.token_address] = (
                    events_by_token_address.get(log.event_info.token_address, [])
                )
                events_by_token_address[log.event_info.token_address].append(log)

            console.log(
                f"Token accounting: Starting at {(token_accounting_last_processed_block):,.0f}, I found {len(result):,.0f} logged events on {self.net} to process from {len(list(events_by_token_address.keys())):,.0f} token addresses."
            )

            # Retrieve the token_addresses for all from the collection
            token_addresses_as_class_initial = {
                x["_id"]: MongoTypeTokenAddress(**x)
                for x in self.db[Collections.tokens_token_addresses_v2].find(
                    {"_id": {"$in": list(events_by_token_address.keys())}}
                )
            }

            links_to_save = []

            token_addresses_to_save = []
            for log in result:
                log: MongoTypeLoggedEventV2
                if log.event_info.token_address not in token_addresses_as_class_initial:
                    token_address_as_class = self.create_new_token_address_v2(
                        log.event_info.token_address, log.tx_info.block_height
                    )
                    token_addresses_to_update[log.event_info.token_address] = (
                        token_address_as_class
                    )

                contract_ = log.event_info.contract

                if log.recognized_event.tag == 252:
                    # this is an operatorUpdate event, doesn't have a token_id, nothing to do here.
                    continue

                token_id_ = log.recognized_event.token_id
                addresses_to_save = []
                if log.recognized_event.tag == 255:
                    addresses_to_save.append(log.recognized_event.from_address)
                    addresses_to_save.append(log.recognized_event.to_address)
                elif log.recognized_event.tag == 254:
                    addresses_to_save.append(log.recognized_event.to_address)
                elif log.recognized_event.tag == 253:
                    addresses_to_save.append(log.recognized_event.from_address)
                elif log.recognized_event.tag == 251:
                    if (
                        log.event_info.token_address
                        not in token_addresses_as_class_initial
                    ):
                        token_address_as_class = self.create_new_token_address_v2(
                            log.event_info.token_address, log.tx_info.block_height
                        )
                        token_addresses_to_update[log.event_info.token_address] = (
                            token_address_as_class
                        )

                    else:
                        token_address_as_class = token_addresses_as_class_initial[
                            log.event_info.token_address
                        ]

                    token_address_as_class.metadata_url = (
                        log.recognized_event.metadata.url
                    )
                    token_addresses_to_update[log.event_info.token_address] = (
                        token_address_as_class
                    )
                    # save_token_address = True

                for address in list(set(addresses_to_save)):
                    if address is None:
                        continue

                    _id = f"{contract_}-{token_id_}-{address}"
                    token_holding = MongoTypeTokenForAddress(
                        **{
                            "token_address": f"{contract_}-{token_id_}",
                            "contract": contract_,
                            "token_id": token_id_,
                            "token_amount": 0,
                        }
                    )

                    link_to_save = MongoTypeTokenLink(
                        **{
                            "_id": _id,
                            "account_address": address,
                            "account_address_canonical": address[:29],
                        }
                    )
                    link_to_save.token_holding = token_holding
                    repl_dict = link_to_save.model_dump(exclude_none=True)
                    if "id" in repl_dict:
                        del repl_dict["id"]
                    links_to_save.append(
                        ReplaceOne({"_id": _id}, repl_dict, upsert=True)
                    )

            for ta in token_addresses_to_update.values():
                ta: MongoTypeTokenAddress
                repl_dict = ta.model_dump(exclude_none=True)
                if "id" in repl_dict:
                    del repl_dict["id"]

                token_addresses_to_save.append(
                    ReplaceOne(
                        {"_id": ta.id},
                        replacement=repl_dict,
                        upsert=True,
                    )
                )
                self.mqtt.publish(
                    f"ccdexplorer/{self.net}/metadata/fetch",
                    json.dumps(repl_dict),
                    qos=MQTT_QOS,
                )

            if len(links_to_save) > 0:
                result2 = self.db[Collections.tokens_links_v3].bulk_write(links_to_save)
                console.log(
                    f"TL:  {len(links_to_save):5,.0f} | M {result2.matched_count:5,.0f} | Mod {result2.modified_count:5,.0f} | U {result2.upserted_count:5,.0f}"
                )
                # if result2.upserted_count > 0:
                #     console.log(result2.upserted_ids)
            if len(token_addresses_to_save) > 0:
                result3 = self.db[Collections.tokens_token_addresses_v2].bulk_write(
                    token_addresses_to_save
                )
                console.log(
                    f"TA:  {len(token_addresses_to_save):5,.0f} | M {result3.matched_count:5,.0f} | Mod {result3.modified_count:5,.0f} | U {result3.upserted_count:5,.0f}"
                )

            self.log_last_token_accounted_message_in_mongo(
                token_accounting_last_processed_block_when_done
            )

    def create_new_token_address_v2(
        self, token_address: str, height: int
    ) -> MongoTypeTokenAddressV2:
        instance_address = token_address.split("-")[0]
        token_id = token_address.split("-")[1]
        token_address = MongoTypeTokenAddressV2(
            **{
                "_id": token_address,
                "contract": instance_address,
                "token_id": token_id,
                "token_amount": str(int(0)),  # mongo limitation on int size
                "last_height_processed": height,
                "hidden": False,
            }
        )
        return token_address

    ########### Token Accounting
