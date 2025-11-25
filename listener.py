from web3 import Web3
from web3.providers.rpc import HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware  # Necessary for POA chains
from pathlib import Path
import json
from datetime import datetime
import pandas as pd
import os


def scan_blocks(chain, start_block, end_block, contract_address, eventfile='deposit_logs.csv'):
    """
    chain - string (Either 'bsc' or 'avax')
    start_block - integer first block to scan
    end_block - integer last block to scan
    contract_address - the address of the deployed contract

    This function reads "Deposit" events from the specified contract,
    and writes information about the events to the file "deposit_logs.csv"
    """
    # RPC endpoints
    if chain == 'avax':
        api_url = "https://api.avax-test.network/ext/bc/C/rpc"  # AVAX C-chain testnet
    elif chain == 'bsc':
        api_url = "https://data-seed-prebsc-1-s1.binance.org:8545/"  # BSC testnet
    else:
        raise ValueError("chain must be 'avax' or 'bsc'")

    # Web3 setup
    w3 = Web3(Web3.HTTPProvider(api_url))
    # Inject POA middleware for testnets using Clique/IBFT
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    # Event ABI (note: parameter name 'recipient' must match the event in your contract)
    DEPOSIT_ABI = json.loads(
        '[{"anonymous":false,"inputs":['
        '{"indexed":true,"internalType":"address","name":"token","type":"address"},'
        '{"indexed":true,"internalType":"address","name":"recipient","type":"address"},'
        '{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],'
        '"name":"Deposit","type":"event"}]'
    )
    contract = w3.eth.contract(address=contract_address, abi=DEPOSIT_ABI)

    arg_filter = {}

    # Resolve latest
    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()

    if end_block < start_block:
        raise ValueError(f"end_block ({end_block}) < start_block ({start_block})")

    # Logging
    if start_block == end_block:
        print(f"Scanning block {start_block} on {chain}")
    else:
        print(f"Scanning blocks {start_block} - {end_block} on {chain}")

    # Helper to write rows to CSV
    def write_rows(rows, write_header):
        if not rows:
            df_empty = pd.DataFrame(columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
            df_empty.to_csv(eventfile, index=False, mode='w' if write_header else 'a', header=True)
        else:
            df = pd.DataFrame(rows, columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
            df['amount'] = df['amount'].astype('int64')
            df.to_csv(eventfile, index=False, mode='w' if write_header else 'a', header=True)

    # Small range: one filter call
    if end_block - start_block < 30:
        event_filter = contract.events.Deposit.create_filter(
            from_block=start_block,
            to_block=end_block,
            argument_filters=arg_filter
        )
        events = event_filter.get_all_entries()

        rows = []
        for evt in events:
            rows.append({
                'chain': chain,
                'token': evt.args['token'],
                'recipient': evt.args['recipient'],
                'amount': int(evt.args['amount']),
                'transactionHash': evt.transactionHash.hex(),
                'address': evt.address,
            })

        write_header = not os.path.exists(eventfile)
        write_rows(rows, write_header)

    # Large range: iterate block by block
    else:
        write_header = not os.path.exists(eventfile)
        for block_num in range(start_block, end_block + 1):
            event_filter = contract.events.Deposit.create_filter(
                from_block=block_num,
                to_block=block_num,
                argument_filters=arg_filter
            )
            events = event_filter.get_all_entries()

            rows = []
            for evt in events:
                rows.append({
                    'chain': chain,
                    'token': evt.args['token'],
                    'recipient': evt.args['recipient'],
                    'amount': int(evt.args['amount']),
                    'transactionHash': evt.transactionHash.hex(),
                    'address': evt.address,
                })

            write_rows(rows, write_header)
            # After the first write, subsequent writes should append without header
            write_header = False
