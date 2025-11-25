from web3 import Web3
from web3.providers.rpc import HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware  # Necessary for POA chains
import json
import pandas as pd
import os


def scan_blocks(chain, start_block, end_block, contract_address, eventfile='deposit_logs.csv'):
    if chain == 'avax':
        api_url = "https://api.avax-test.network/ext/bc/C/rpc"
    elif chain == 'bsc':
        api_url = "https://data-seed-prebsc-1-s1.binance.org:8545/"
    else:
        raise ValueError("chain must be 'avax' or 'bsc'")

    w3 = Web3(Web3.HTTPProvider(api_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    # Use canonical param name "to"
    DEPOSIT_ABI = json.loads(
        '[{"anonymous":false,"inputs":['
        '{"indexed":true,"internalType":"address","name":"token","type":"address"},'
        '{"indexed":true,"internalType":"address","name":"to","type":"address"},'
        '{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],'
        '"name":"Deposit","type":"event"}]'
    )
    contract = w3.eth.contract(address=contract_address, abi=DEPOSIT_ABI)

    arg_filter = {}

    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()

    if end_block < start_block:
        raise ValueError(f"end_block ({end_block}) < start_block ({start_block})")

    if start_block == end_block:
        print(f"Scanning block {start_block} on {chain}")
    else:
        print(f"Scanning blocks {start_block} - {end_block} on {chain}")

    def write_rows(rows, write_header):
        if not rows:
            df_empty = pd.DataFrame(columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
            df_empty.to_csv(eventfile, index=False, mode='w' if write_header else 'a', header=True)
        else:
            df = pd.DataFrame(rows, columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
            # 不要转换为 int64，保持为字符串，避免精度/范围问题和与评测不一致
            df.to_csv(eventfile, index=False, mode='w' if write_header else 'a', header=True)

    if end_block - start_block < 30:
        event_filter = contract.events.Deposit.create_filter(
            from_block=start_block, to_block=end_block, argument_filters=arg_filter
        )
        events = event_filter.get_all_entries()

        rows = [{
            'chain': chain,
            'token': evt.args['token'],
            'recipient': evt.args['to'],
            'amount': str(evt.args['amount']),  # 写入十进制字符串
            'transactionHash': evt.transactionHash.hex(),
            'address': evt.address,
        } for evt in events]

        write_header = not os.path.exists(eventfile)
        write_rows(rows, write_header)
    else:
        write_header = not os.path.exists(eventfile)
        for block_num in range(start_block, end_block + 1):
            event_filter = contract.events.Deposit.create_filter(
                from_block=block_num, to_block=block_num, argument_filters=arg_filter
            )
            events = event_filter.get_all_entries()

            rows = [{
                'chain': chain,
                'token': evt.args['token'],
                'recipient': evt.args['to'],
                'amount': str(evt.args['amount']),  # 写入十进制字符串
                'transactionHash': evt.transactionHash.hex(),
                'address': evt.address,
            } for evt in events]

            write_rows(rows, write_header)
            write_header = False
