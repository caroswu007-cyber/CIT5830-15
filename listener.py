from web3 import Web3
from web3.providers.rpc import HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware  # for POA testnets
import os
import json
import pandas as pd


def scan_blocks(chain, start_block, end_block, contract_address, eventfile='deposit_logs.csv'):
    """
    chain - 'bsc' or 'avax'
    start_block - int or "latest"
    end_block - int or "latest"
    contract_address - contract address
    Writes Deposit events to deposit_logs.csv
    """
    # Select RPC
    if chain == 'avax':
        api_url = "https://api.avax-test.network/ext/bc/C/RPC"
    elif chain == 'bsc':
        api_url = "https://data-seed-prebsc-1-s1.binance.org:8545/"
    else:
        raise ValueError("chain must be 'avax' or 'bsc'")

    w3 = Web3(HTTPProvider(api_url))
    # Inject POA middleware
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    contract_address = Web3.to_checksum_address(contract_address)

    # Minimal ABI: Deposit(address indexed token, address indexed to, uint256 amount)
    DEPOSIT_ABI = json.loads(
        '[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},'
        '{"indexed":true,"internalType":"address","name":"to","type":"address"},'
        '{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],'
        '"name":"Deposit","type":"event"}]'
    )
    contract = w3.eth.contract(address=contract_address, abi=DEPOSIT_ABI)

    # Resolve "latest"
    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()

    if end_block < start_block:
        raise ValueError("end_block must be >= start_block")

    print(f"Scanning blocks {start_block}" + ("" if start_block == end_block else f" - {end_block}") + f" on {chain}")

    rows = []
    arg_filter = {}

    if end_block - start_block < 30:
        event_filter = contract.events.Deposit.create_filter(
            fromBlock=start_block, toBlock=end_block, argument_filters=arg_filter
        )
        events = event_filter.get_all_entries()
        for evt in events:
            rows.append({
                'chain': chain,
                'token': evt.args['token'],
                'recipient': evt.args['to'],
                'amount': int(evt.args['amount']),
                'transactionHash': evt.transactionHash.hex(),
                'address': evt.address,
            })
    else:
        for block_num in range(start_block, end_block + 1):
            event_filter = contract.events.Deposit.create_filter(
                fromBlock=block_num, toBlock=block_num, argument_filters=arg_filter
            )
            events = event_filter.get_all_entries()
            for evt in events:
                rows.append({
                    'chain': chain,
                    'token': evt.args['token'],
                    'recipient': evt.args['to'],
                    'amount': int(evt.args['amount']),
                    'transactionHash': evt.transactionHash.hex(),
                    'address': evt.address,
                })

    # Write CSV
    if not rows:
        write_header = not os.path.exists(eventfile)
        df = pd.DataFrame(columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
        df.to_csv(eventfile, index=False, mode='w' if write_header else 'a', header=True)
        return

    df = pd.DataFrame(rows, columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
    write_header = not os.path.exists(eventfile)
    df.to_csv(eventfile, index=False, mode='w' if write_header else 'a', header=True)
