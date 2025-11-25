from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware  # Necessary for POA chains
import json
import pandas as pd
import os


def scan_blocks(chain, start_block, end_block, contract_address, eventfile='deposit_logs.csv'):
    """
    chain - string (Either 'bsc' or 'avax')
    start_block - integer first block to scan or "latest"
    end_block - integer last block to scan or "latest"
    contract_address - the address of the deployed contract

    This function reads "Deposit" events from the specified contract,
    and writes information about the events to the file "deposit_logs.csv"
    """
    # Select RPC
    if chain == 'avax':
        api_url = "https://api.avax-test.network/ext/bc/C/rpc"  # AVAX C-chain testnet (Fuji)
    elif chain == 'bsc':
        api_url = "https://data-seed-prebsc-1-s1.binance.org:8545/"  # BSC testnet
    else:
        raise ValueError("chain must be 'avax' or 'bsc'")

    # Web3 setup
    w3 = Web3(Web3.HTTPProvider(api_url))
    # Inject POA middleware for testnets using Clique/IBFT
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    # Event ABI: keep names and order exactly as on-chain
    DEPOSIT_ABI = json.loads(
        '[{"anonymous":false,"inputs":['
        '{"indexed":true,"internalType":"address","name":"token","type":"address"},'
        '{"indexed":true,"internalType":"address","name":"recipient","type":"address"},'
        '{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],'
        '"name":"Deposit","type":"event"}]'
    )
    contract = w3.eth.contract(address=contract_address, abi=DEPOSIT_ABI)

    # Resolve latest markers
    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()

    if end_block < start_block:
        raise ValueError(f"end_block ({end_block}) < start_block ({start_block})")

    # Info
    if start_block == end_block:
        print(f"Scanning block {start_block} on {chain}")
    else:
        print(f"Scanning blocks {start_block} - {end_block} on {chain}")

    # Helper: write rows to CSV
    columns = ['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address']

    def ensure_header_if_needed(write_header_flag: bool):
        if write_header_flag and not os.path.exists(eventfile):
            # Create file with header even if there are no rows in first run
            pd.DataFrame(columns=columns).to_csv(eventfile, index=False, mode='w', header=True)

    def write_rows(rows, write_header_flag: bool):
        if not rows:
            ensure_header_if_needed(write_header_flag)
            return
        df = pd.DataFrame(rows, columns=columns)
        # Do not cast to int64; Python int preserves big integers and pandas writes as plain integers
        df.to_csv(eventfile, index=False, mode='w' if write_header_flag else 'a', header=write_header_flag)

    arg_filter = {}  # extend here if you want to filter by indexed params

    # Small range: single filter call
    if end_block - start_block < 30:
        event_filter = contract.events.Deposit.create_filter(
            from_block=start_block,
            to_block=end_block,
            argument_filters=arg_filter
        )
        events = event_filter.get_all_entries()

        rows = []
        for evt in events:
            # Optional debug:
            # print("DEBUG evt:", evt.transactionHash.hex(), dict(evt.args), "addr:", evt.address)
            rows.append({
                'chain': chain,
                'token': evt.args['token'],
                'recipient': evt.args['recipient'],
                'amount': int(evt.args['amount']),            # keep as Python big int
                'transactionHash': evt.transactionHash.hex(),
                'address': evt.address,
            })

        write_header = not os.path.exists(eventfile)
        write_rows(rows, write_header)

    # Large range: iterate block-by-block to avoid provider limits
    else:
        write_header = not os.path.exists(eventfile)
        # Ensure header exists even if the first few blocks have no events
        ensure_header_if_needed(write_header)
        for block_num in range(start_block, end_block + 1):
            event_filter = contract.events.Deposit.create_filter(
                from_block=block_num,
                to_block=block_num,
                argument_filters=arg_filter
            )
            events = event_filter.get_all_entries()

            rows = []
            for evt in events:
                # Optional debug:
                # print("DEBUG evt:", evt.transactionHash.hex(), dict(evt.args), "addr:", evt.address)
                rows.append({
                    'chain': chain,
                    'token': evt.args['token'],
                    'recipient': evt.args['recipient'],
                    'amount': int(evt.args['amount']),        # keep as Python big int
                    'transactionHash': evt.transactionHash.hex(),
                    'address': evt.address,
                })

            write_rows(rows, write_header)
            # After first write, subsequent appends should not include header
            write_header = False
