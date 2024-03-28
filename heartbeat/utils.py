from enum import Enum

from ccdexplorer_fundamentals.GRPCClient.CCD_Types import CCD_BlockInfo
from ccdexplorer_fundamentals.mongodb import Collections


class Queue(Enum):
    """
    Type of queue to store messages in to send to MongoDB.
    Names correspond to the collection names.
    """

    logged_events = 8
    token_addresses_to_redo_accounting = 9
    token_accounts = 13
    token_addresses = 14
    token_links = 15


class Utils:

    def log_last_token_accounted_message_in_mongo(self, height: int):
        query = {"_id": "token_accounting_last_processed_block_v2"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": "token_accounting_last_processed_block_v2",
                "height": height,
            },
            upsert=True,
        )

    def log_error_in_mongo(self, e, current_block_to_process: CCD_BlockInfo):
        query = {"_id": f"block_failure_{current_block_to_process.height}"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": f"block_failure_{current_block_to_process.height}",
                "height": current_block_to_process.height,
                "Exception": e,
            },
            upsert=True,
        )
