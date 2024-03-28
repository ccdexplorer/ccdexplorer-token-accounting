# CCDExplorer Token Accounting

This repo performs token accounting for CCDExplorer.io.

All methods below get called on a schedule, are designed to work independently and write their results to a set of MongoDB collections. Note that all documents we write to collections have predictable `_ids`, which makes it easy to redo (parts of) a process.

Methods:
1. [Update Token Accounting](#method-token-accounting)
2. [Get Redo Token Addresses](#method-get-redo-token-addresses)
3. [Special Purpose Token Accounting](#method-special-purpose-token-accounting)


## Method: Token Accounting

Token accounting is the process of accounting for mints, burns and transfers for CIS-2 tokens. These tokens are not stored on-chain. Instead, account holdings can only be deduced from the `logged events`. Therefore it is very important that logged events are stored correctly, with no omissions and duplications. Also, the order in which logged events are applied, matters, as you can't burn or transfer tokens you do not own. 

Check [CIS-2 Specification for logged events](http://proposals.concordium.software/CIS/cis-2.html#logged-events).

### Relevant Collections
Below are example documents as they are stored in the respective collections. 

#### tokens_logged_events
This collection stores logged events.
``` py
{
  "_id": "5351380-<8586,0>-45000000-fe0445000000010043a0c163c7f7a8e58ba325fde7b504153592ded8507f75264fb4fc4b30000002-updated-23-0-0",
  "logged_event": "fe0445000000010043a0c163c7f7a8e58ba325fde7b504153592ded8507f75264fb4fc4b30000002",
  "result": {
    "tag": 254,
    "token_id": "45000000",
    "token_amount": "1",
    "to_address": "3TXeDWoBvHQpn7uisvRP8miX47WF4tpkKBidM4MLsDPeshUTs8"
  },
  "tag": 254,
  "event_type": "mint_event",
  "block_height": 5351380,
  "tx_hash": "2c0a2e67766c41c8d4c2484db5a9804f937ab862f8528639522d2cb1bb152e18",
  "tx_index": 23,
  "ordering": 1,
  "token_address": "<8586,0>-45000000",
  "contract": "<8586,0>"
}
```

#### tokens_token_addresses
This collection stores individual tokens.
``` py
{
  "_id": "<9403,0>-01288764e78695027bd972e9b654cde28df2563e56b3ed66a4c8f4dcb3c08cec",
  "contract": "<9403,0>",
  "token_id": "01288764e78695027bd972e9b654cde28df2563e56b3ed66a4c8f4dcb3c08cec",
  "token_amount": "1",
  "metadata_url": "https://nft.ptags.io/01288764E78695027BD972E9B654CDE28DF2563E56B3ED66A4C8F4DCB3C08CEC",
  "last_height_processed": 14189078,
  "token_metadata": {
    "name": "Psi 2009",
    "unique": true,
    "description": "Psi 2009",
    "thumbnail": {
      "url": "https://storage.googleapis.com/provenance_images/82_4ce5e65e-23a3-47fe-bc5e-109856fa700d.png"
    },
    "display": {
      "url": "https://storage.googleapis.com/provenance_images/82_4ce5e65e-23a3-47fe-bc5e-109856fa700d.png"
    },
    "attributes": [
      {
        "type": "string",
        "name": "nfc_id",
        "value": "01288764E78695027BD972E9B654CDE28DF2563E56B3ED66A4C8F4DCB3C08CEC"
      }
    ]
  },
  "hidden": false
}
```

#### tokens_links
This collections stores links between accounts and tokens.
``` py
{
  "_id": "<8586,0>-50000000-3TXeDWoBvHQpn7uisvRP8miX47WF4tpkKBidM4MLsDPgXn9Rgv",
  "account_address": "3TXeDWoBvHQpn7uisvRP8miX47WF4tpkKBidM4MLsDPgXn9Rgv",
  "account_address_canonical": "3TXeDWoBvHQpn7uisvRP8miX47WF4",
  "token_holding": {
    "token_address": "<8586,0>-50000000",
    "contract": "<8586,0>",
    "token_id": "50000000",
    "token_amount": "1"
  }
}
```

#### tokens_tags
This collection stores recognized tokens/contracts and summarizes this into tags.
``` py
{
  "_id": "USDT",
  "contracts": [
    "<9341,0>"
  ],
  "tag_template": false,
  "single_use_contract": true,
  "logo_url": "https://cryptologos.cc/logos/tether-usdt-logo.svg?v=025",
  "decimals": {
    "$numberLong": "6"
  },
  "owner": "Arabella",
  "module_name": "cis2-bridgeable"
}
```

The starting point is reading the helper document `token_accounting_last_processed_block`. This value indicates the last block that was processed for logged events. Hence, if we start at logged events after this block, there is no double counting. If this value is either not present or set to -1, all token_addresses (and associated token_accounts) will be reset. 

**Need to redo token accounting?**: Set helper document `token_accounting_last_processed_block` to -1. 


We collect all logged events from the collection `tokens_logged_events` with the following query:

``` py
{"block_height": {"$gt": token_accounting_last_processed_block}}
.sort(
    [
        ("block_height", ASCENDING),
        ("tx_index", ASCENDING),
        ("ordering", ASCENDING),
    ]
).limit(1000)
```

If there are `logged_events` to process, we sort the events into a dict `events_by_token_address`, keyed on token_address and contains an ordered list of logged events related to this token_address.

Next, we retrieve the token_addresses from the collection for all token addresses that are mentioned in the logged events. 
Finally, we retrieve all token_links from the collection for all token addresses that are mentioned in the logged events. 
``` py                    
events_by_token_address: dict[str, list] = {}
for log in result:
    events_by_token_address[log.token_address] = (
        events_by_token_address.get(log.token_address, [])
    )
    events_by_token_address[log.token_address].append(log)
```


We then loop through all `token_addresses` that have logged events to process and call `token_accounting_for_token_address`. 

The method `token_accounting_for_token_address` first deterines the need to create a new token address (when we are starting over with token accounting, setting `token_accounting_last_processed_block` to -1, or if the `token_address` doesn't exist).

Then for this `token_address`, we loop through all logged events that need processing, and call `execute_logged_event`. 

This method preforms the neccesary actions on the `token_address_as_class` variable. Once all logged events for a `token_address` are executed, we save the result back to the collection `tokens_token_addresses`. 

Note that token holder information is stored in the `tokens_links` collection.

Finally, after all logged events are processed for all token addresses, write back to the helper collection for `_id`: `token_accounting_last_processed_block` the block_height of the last logged event. 