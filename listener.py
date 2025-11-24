from web3 import Web3
from web3.providers.rpc import HTTPProvider
from web3.middleware import geth_poa_middleware as ExtraDataToPOAMiddleware  # Necessary for POA chains
import os
import json
import pandas as pd


def scan_blocks(chain, start_block, end_block, contract_address, eventfile='deposit_logs.csv'):
    """
    chain - string (Either 'bsc' or 'avax')
    start_block - integer first block to scan
    end_block - integer last block to scan
    contract_address - the address of the deployed contract

    This function reads "Deposit" events from the specified contract,
    and writes information about the events to the file "deposit_logs.csv"
    """

    # Pick RPC by chain
    if chain == 'avax':
        api_url = "https://api.avax-test.network/ext/bc/C/RPC"  # AVAX C-Chain testnet
    elif chain == 'bsc':
        api_url = "https://data-seed-prebsc-1-s1.binance.org:8545/"  # BSC testnet
    else:
        raise ValueError("chain must be 'avax' or 'bsc'")

    # Web3 with POA middleware for these testnets
    w3 = Web3(HTTPProvider(api_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    contract_address = Web3.to_checksum_address(contract_address)

    # Minimal ABI for Deposit(address indexed token, address indexed to, uint256 amount)
    DEPOSIT_ABI = json.loads(
        '[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token","type":"address"},'
        '{"indexed":true,"internalType":"address","name":"to","type":"address"},'
        '{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],'
        '"name":"Deposit","type":"event"}]'
    )
    contract = w3.eth.contract(address=contract_address, abi=DEPOSIT_ABI)

    # Handle "latest"
    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()

    if end_block < start_block:
        raise ValueError("end_block must be >= start_block")

    if start_block == end_block:
        print(f"Scanning block {start_block} on {chain}")
    else:
        print(f"Scanning blocks {start_block} - {end_block} on {chain}")

    arg_filter = {}
    rows = []

    # Small range: single query; otherwise per-block to avoid timeouts
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

    # Ensure CSV exists and headers are written even if no events found
    if not rows:
        write_header = not os.path.exists(eventfile)
        df = pd.DataFrame(columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
        df.to_csv(eventfile, index=False, mode='w' if write_header else 'a', header=True)
        return

    # Write rows
    df = pd.DataFrame(rows, columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address'])
    write_header = not os.path.exists(eventfile)
    df.to_csv(eventfile, index=False, mode='w' if write_header else 'a', header=True)
