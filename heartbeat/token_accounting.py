import asyncio
import datetime as dt
from datetime import timezone

import aiohttp
from ccdexplorer_fundamentals.cis import (
    MongoTypeLoggedEventV2,
    FailedAttempt,
    # MongoTypeLoggedEvent,
    MongoTypeTokenAddress,
    MongoTypeTokenForAddress,
    MongoTypeTokenHolderAddress,
    MongoTypeTokenLink,
    TokenMetaData,
    burnEvent,
    mintEvent,
    tokenMetadataEvent,
    transferEvent,
)
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import CCD_AccountAddress
from ccdexplorer_fundamentals.mongodb import (
    Collections,
)
from pymongo import ASCENDING, DeleteOne, ReplaceOne
from pymongo.collection import Collection
from rich.console import Console

from .utils import Queue, Utils

console = Console()


########### Token Accounting V3
class TokenAccounting(Utils):
    async def update_token_accounting(self):
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
        try:
            while self.sending:
                await asyncio.sleep(0.3)
                print("waiting for sending to finish")
            start = dt.datetime.now()
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
                        # "tx_info.tx_index": ASCENDING,
                        # "event_info.effect_index": ASCENDING,
                        # "event_info.event_index": ASCENDING,
                    }
                },
                {"$limit": 10_000},
            ]
            result: list[MongoTypeLoggedEventV2] = [
                MongoTypeLoggedEventV2(**x)
                for x in self.db[Collections.tokens_logged_events_v2].aggregate(
                    pipeline
                )
            ]

            # Only continue if there are logged events to process...
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
                    f"Token accounting: Starting at {(token_accounting_last_processed_block+1):,.0f}, I found {len(result):,.0f} logged events on {self.net} to process from {len(list(events_by_token_address.keys())):,.0f} token addresses."
                )

                # Looping through all token_addresses that have logged_events

                # Retrieve the token_addresses for all from the collection
                token_addresses_as_class_from_collection = {
                    x["_id"]: MongoTypeTokenAddress(**x)
                    for x in self.db[Collections.tokens_token_addresses_v3].find(
                        {"_id": {"$in": list(events_by_token_address.keys())}}
                    )
                }

                # Retrieve all current links for this set of token_addresses.
                token_links_from_collection_result = list(
                    self.db[Collections.tokens_links_v2].find(
                        {
                            "token_holding.token_address": {
                                "$in": list(events_by_token_address.keys())
                            }
                        }
                    )
                )
                token_links_from_collection_by_token_address = {}
                for link in token_links_from_collection_result:
                    link_token_address = link["token_holding"]["token_address"]
                    link_account_address = link["account_address"]
                    if not token_links_from_collection_by_token_address.get(
                        link_token_address
                    ):
                        token_links_from_collection_by_token_address[
                            link_token_address
                        ] = {}

                    # Double dict, so first lookup token address, then account address.
                    token_links_from_collection_by_token_address[link_token_address][
                        link_account_address
                    ] = MongoTypeTokenLink(**link)

                for index, token_address in enumerate(
                    list(events_by_token_address.keys())
                ):
                    self.token_accounting_for_token_address(
                        token_address,
                        events_by_token_address,
                        token_addresses_as_class_from_collection,
                        token_links_from_collection_by_token_address,
                        token_accounting_last_processed_block,
                    )

                self.send_token_queues_to_mongo(0)
                self.log_last_token_accounted_message_in_mongo(
                    token_accounting_last_processed_block_when_done
                )
                end = dt.datetime.now()
                # console.log(
                #     f"update token accounting for {len(result):,.0f} events took {(end-start).total_seconds():,.3f}s"
                # )
        except Exception as e:
            console.log(e)

            # await asyncio.sleep(1)

    def send_token_queues_to_mongo(self, limit: int = 0):
        self.queues: dict[Collections, list]
        self.sending = True
        if len(self.queues[Queue.token_addresses]) > limit:
            result1 = self.db[Collections.tokens_token_addresses_v3].bulk_write(
                self.queues[Queue.token_addresses]
            )
            console.log(
                f"TA:  {len(self.queues[Queue.token_addresses]):5,.0f} | M {result1.matched_count:5,.0f} | Mod {result1.modified_count:5,.0f} | U {result1.upserted_count:5,.0f}"
            )

            self.queues[Queue.token_addresses] = []

        if len(self.queues[Queue.token_links]) > limit:
            result2 = self.db[Collections.tokens_links_v3].bulk_write(
                self.queues[Queue.token_links]
            )
            console.log(
                f"TL:  {len(self.queues[Queue.token_links]):5,.0f} | M {result2.matched_count:5,.0f} | Mod {result2.modified_count:5,.0f} | U {result2.upserted_count:5,.0f}"
            )

            self.queues[Queue.token_links] = []
        self.sending = False

    def token_accounting_for_token_address(
        self,
        token_address: str,
        events_by_token_address: dict,
        token_addresses_as_class_from_collection: dict,
        token_links_from_collection_by_token_address: dict,
        token_accounting_last_processed_block: int = -1,
    ):
        self.queues: dict[Collections, list]
        queue = []
        # if we start at the beginning of the chain for token accounting
        # create an empty token address as class to start
        if token_accounting_last_processed_block == -1:
            # create new empty token_address in memory
            # we will overwrite the token address in the collection with this.
            token_address_as_class = self.create_new_token_address(token_address)
            # print(token_address)
            # remove any links to this address from the collection.
            _ = self.db[Collections.tokens_links_v2].delete_many(
                {"token_holding.token_address": token_address_as_class.id}
            )

        else:
            # Retrieve the token_address document from the collection
            token_address_as_class = token_addresses_as_class_from_collection.get(
                token_address
            )

            # If it's not there, create an new token_address
            if not token_address_as_class:
                token_address_as_class = self.create_new_token_address(token_address)
            else:
                # make sure the token_address_as_call is actually typed correctly.
                if type(token_address_as_class) is not MongoTypeTokenAddress:
                    token_address_as_class = MongoTypeTokenAddress(
                        **token_address_as_class
                    )
                # Need to read in the current token holders from the links collection
                current_token_holders = (
                    token_links_from_collection_by_token_address.get(token_address)
                )
                if current_token_holders:
                    token_address_as_class.token_holders = {
                        x.account_address: x.token_holding.token_amount
                        for x in token_links_from_collection_by_token_address.get(
                            token_address
                        ).values()
                    }
                else:
                    token_address_as_class.token_holders = {}

        # This is the list of logged events for the selected token_address
        logs_for_token_address: MongoTypeLoggedEventV2 = events_by_token_address[
            token_address
        ]
        for log in logs_for_token_address:
            log: MongoTypeLoggedEventV2
            # Perform token accounting for this logged event
            # This function works on and returns 'token_address_as_class'.
            token_address_as_class = self.execute_logged_event(
                token_address_as_class,
                log,
            )

        # Set the last block_height that affected the token accounting
        # for this token_address to the last logged event block_height.
        token_address_as_class.last_height_processed = log.tx_info.block_height

        queue = self.copy_token_holders_to_links(
            token_address_as_class, token_links_from_collection_by_token_address
        )
        self.queues[Queue.token_links].extend(queue)

        # Write the token_address_as_class back to the collection.
        # now token holders information is stored in links
        if (
            token_address_as_class.token_id
            == "eb9ed27170796089cd3d1cfc445b197686446457bfe7657c2f6f72df01559255"
        ):
            print(token_address_as_class)
            # only save token addresses that have token hold
        self.queues[Queue.token_addresses].append(
            self.mongo_save_for_token_address(token_address_as_class)
        )
        #

    def mongo_save_for_token_address(
        self, token_address_as_class: MongoTypeTokenAddress
    ):
        repl_dict = token_address_as_class.model_dump(exclude_none=True)
        if "id" in repl_dict:
            del repl_dict["id"]

        # remove token holders again, as that's only an intermediate result.
        # actual holding are stored in the link collection.
        if "token_holders" in repl_dict:
            del repl_dict["token_holders"]

        queue_item = ReplaceOne(
            {"_id": token_address_as_class.id},
            replacement=repl_dict,
            upsert=True,
        )
        return queue_item

    def copy_token_holders_to_links(
        self,
        token_address_as_class: MongoTypeTokenAddress,
        token_links_from_collection_by_token_address: dict,
    ):
        _queue = []
        for address, token_amount in token_address_as_class.token_holders.items():
            # address_to_save = self.db[Collections.tokens_accounts].find_one(
            #     {"_id": address}
            # )
            account_address_to_save = token_links_from_collection_by_token_address.get(
                address
            )
            # if this account does not exist yet, create empty dict.
            if not account_address_to_save:
                link_to_save = MongoTypeTokenLink(
                    **{
                        "_id": f"{token_address_as_class.id}-{address}",
                        "account_address": address,
                        "account_address_canonical": address[:29],
                        # "token_holding": address_as_class,
                    }
                )
            else:
                if type(link_to_save) is not MongoTypeTokenHolderAddress:
                    link_to_save = MongoTypeTokenHolderAddress(**link_to_save)

            token_to_save = MongoTypeTokenForAddress(
                **{
                    "token_address": token_address_as_class.id,
                    "contract": token_address_as_class.contract,
                    "token_id": token_address_as_class.token_id,
                    "token_amount": str(token_amount),
                }
            )

            link_to_save.token_holding = token_to_save

            repl_dict = link_to_save.model_dump()
            if "id" in repl_dict:
                del repl_dict["id"]

            if int(token_amount) == 0:
                queue_item = DeleteOne({"_id": link_to_save.id})
            else:
                queue_item = ReplaceOne(
                    {"_id": link_to_save.id},
                    replacement=repl_dict,
                    upsert=True,
                )

            _queue.append(queue_item)

        return _queue

    def save_mint(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEventV2
    ):
        # result = mintEvent(**log.recognized_event)
        result = log.recognized_event

        token_holders: dict[CCD_AccountAddress, str] = (
            token_address_as_class.token_holders
        )
        if result.to_address:
            token_holders[result.to_address] = str(
                int(token_holders.get(result.to_address, "0")) + result.token_amount
            )
        token_address_as_class.token_amount = str(
            (int(token_address_as_class.token_amount) + result.token_amount)
        )
        token_address_as_class.token_holders = token_holders
        return token_address_as_class

    def save_metadata(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEventV2
    ):
        # result = tokenMetadataEvent(**log.recognized_event)
        result = log.recognized_event
        token_address_as_class.metadata_url = result.metadata.url
        # this is very time consuming. Provenance tags with 1000 mints+metadata
        # per tx is killing this.
        # Metadata is hopefully picked up in the main process.
        # await self.read_and_store_metadata(token_address_as_class)
        return token_address_as_class

    def save_transfer(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEventV2
    ):
        # result = transferEvent(**log.recognized_event)
        result = log.recognized_event
        try:
            token_holders: dict[CCD_AccountAddress, str] = (
                token_address_as_class.token_holders
            )
        except:  # noqa: E722
            console.log(
                f"{result.tag}: {token_address_as_class.token_id} | {token_address_as_class} has no field token_holders?"
            )

        if result.to_address:
            token_holders[result.to_address] = str(
                int(token_holders.get(result.to_address, "0")) + result.token_amount
            )
        if result.from_address:
            token_holders[result.from_address] = str(
                int(token_holders.get(result.from_address, "0")) - result.token_amount
            )

        token_address_as_class.token_holders = token_holders
        return token_address_as_class

    def save_burn(
        self, token_address_as_class: MongoTypeTokenAddress, log: MongoTypeLoggedEventV2
    ):
        # result = burnEvent(**log.recognized_event)
        result = log.recognized_event
        token_holders: dict[CCD_AccountAddress, str] = (
            token_address_as_class.token_holders
        )
        if result.from_address:
            token_holders[result.from_address] = str(
                int(token_holders.get(result.from_address, "0")) - result.token_amount
            )

        token_address_as_class.token_amount = str(
            (int(token_address_as_class.token_amount) - result.token_amount)
        )
        token_address_as_class.token_holders = token_holders

        token_address_as_class.token_holders = token_holders
        return token_address_as_class

    def create_new_token_address(self, token_address: str) -> MongoTypeTokenAddress:
        instance_address = token_address.split("-")[0]
        token_id = token_address.split("-")[1]
        token_address = MongoTypeTokenAddress(
            **{
                "_id": token_address,
                "contract": instance_address,
                "token_id": token_id,
                "token_amount": str(int(0)),  # mongo limitation on int size
                # not that we need to include the token_holders here, because we use it in code (but do not store it!)
                "token_holders": {},  # {CCD_AccountAddress, str(token_amount)}
                "last_height_processed": -1,
                "hidden": False,
            }
        )
        return token_address

    def execute_logged_event(
        self,
        token_address_as_class: MongoTypeTokenAddress,
        log: MongoTypeLoggedEventV2,
    ):
        if log.recognized_event.tag == 255:
            token_address_as_class = self.save_transfer(
                token_address_as_class,
                log,
            )
        elif log.recognized_event.tag == 254:
            token_address_as_class = self.save_mint(
                token_address_as_class,
                log,
            )
        elif log.recognized_event.tag == 253:
            token_address_as_class = self.save_burn(
                token_address_as_class,
                log,
            )
        elif log.recognized_event.tag == 251:
            token_address_as_class = self.save_metadata(token_address_as_class, log)

        return token_address_as_class

    ########### Token Accounting
