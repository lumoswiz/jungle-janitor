import os
from typing import Annotated, Dict, List, Tuple

import numpy as np
import pandas as pd
from ape import Contract, chain
from ape.api import BlockAPI
from ape.types import ContractLog
from ape_ethereum import multicall
from silverback import BotState, SilverbackBot
from taskiq import Context, TaskiqDepends, TaskiqState

# Initialize bot
bot = SilverbackBot()

# Contracts
POOL_ADDRESSES_PROVIDER = Contract(os.environ["POOL_ADDRESSES_PROVIDER"])
POOL = Contract(POOL_ADDRESSES_PROVIDER.getPool())
UI_POOL_DATA_PROVIDER_V3 = Contract(os.environ["UI_POOL_DATA_PROVIDER_V3"])

# File paths
BORROWERS_FILEPATH = os.environ.get("BORROWERS_FILEPATH", ".db/borrowers.csv")
BLOCK_FILEPATH = os.environ.get("BLOCK_FILEPATH", ".db/block.csv")

# Constants
MAX_UINT = 2**256 - 1
START_BLOCK = int(os.environ.get("START_BLOCK", chain.blocks.head.number))
MAX_LIQUIDATION_HF_THRESHOLD = 0.95 * 10**18
LIQUIDATION_HF_THRESHOLD = 1 * 10**18
AT_RISK_HF_THRESHOLD = 1.5 * 10**18
AT_RISK_BLOCK_CHECK = 10
REGULAR_BLOCK_CHECK = 75
MULTICALL_BATCH_SIZE = 50


def _load_borrowers_db() -> Dict:
    dtype = {
        "borrower_address": str,
        "health_factor": object,
        "last_hf_update": np.int64,
    }
    df = (
        pd.read_csv(BORROWERS_FILEPATH, dtype=dtype)
        if os.path.exists(BORROWERS_FILEPATH)
        else pd.DataFrame(columns=dtype.keys()).astype(dtype)
    )
    return df.set_index("borrower_address").to_dict("index")


def _load_block_db() -> Dict:
    df = (
        pd.read_csv(BLOCK_FILEPATH)
        if os.path.exists(BLOCK_FILEPATH)
        else pd.DataFrame({"last_processed_block": [START_BLOCK]})
    )
    return {"last_processed_block": df["last_processed_block"].iloc[0]}


def _save_borrowers_db(data: Dict):
    os.makedirs(os.path.dirname(BORROWERS_FILEPATH), exist_ok=True)
    df = pd.DataFrame.from_dict(data, orient="index").reset_index()
    df.columns = ["borrower_address", "health_factor", "last_hf_update"]
    df.to_csv(BORROWERS_FILEPATH, index=False)


def _save_block_db(data: Dict):
    os.makedirs(os.path.dirname(BLOCK_FILEPATH), exist_ok=True)
    df = pd.DataFrame([data])
    df.to_csv(BLOCK_FILEPATH, index=False)


def _update_user_data(address, log, context):
    if address in context.state.borrowers:
        *_, health_factor = POOL.getUserAccountData(address)
        if health_factor == 2**256 - 1:
            del context.state.borrowers[address]
        else:
            context.state.borrowers[address].update(
                {
                    "health_factor": str(health_factor),
                    "last_hf_update": log.block_number,
                }
            )
        _save_borrowers_db(context.state.borrowers)


def _get_unique_borrowers_from_logs(start_block: int, stop_block: int) -> Dict[str, int]:
    logs = POOL.Borrow.range(start_or_stop=start_block, stop=stop_block)
    return {log.onBehalfOf: log.block_number for log in logs}


def _get_borrowers_health_factors(borrowers_addresses: List[str]) -> List[Tuple]:
    call = multicall.Call()
    for borrower in borrowers_addresses:
        call.add(POOL.getUserAccountData, borrower)

    return [
        (borrower, result[-1])
        for borrower, result in zip(borrowers_addresses, call())
        if result is not None and result[-1] != MAX_UINT
    ]


def _process_historical_events(start_block: int, stop_block: int) -> None:
    borrowers = _get_unique_borrowers_from_logs(start_block, stop_block)
    borrower_addresses = list(borrowers.keys())

    for i in range(0, len(borrower_addresses), MULTICALL_BATCH_SIZE):
        batch = borrower_addresses[i : i + MULTICALL_BATCH_SIZE]

        results = _get_borrowers_health_factors(batch)

        current_borrowers = _load_borrowers_db()

        for borrower, health_factor in results:
            current_borrowers[borrower] = {
                "health_factor": str(health_factor),
                "last_hf_update": borrowers[borrower],
            }

        _save_borrowers_db(current_borrowers)


def _sync_health_factors(context: Context, current_block: int) -> Dict:
    at_risk_borrowers = [
        address
        for address, data in context.state.borrowers.items()
        if (
            int(data["health_factor"]) < AT_RISK_HF_THRESHOLD
            and current_block - data["last_hf_update"] > AT_RISK_BLOCK_CHECK
        )
    ]

    safe_borrowers = [
        address
        for address, data in context.state.borrowers.items()
        if (
            int(data["health_factor"]) >= AT_RISK_HF_THRESHOLD
            and current_block - data["last_hf_update"] > REGULAR_BLOCK_CHECK
        )
    ]

    borrowers_to_check = at_risk_borrowers + safe_borrowers
    if not borrowers_to_check:
        return {
            "updated_count": 0,
            "at_risk_checked": 0,
            "safe_checked": 0,
            "total_checked": 0,
        }

    updated_count = 0
    for i in range(0, len(borrowers_to_check), MULTICALL_BATCH_SIZE):
        batch = borrowers_to_check[i : i + MULTICALL_BATCH_SIZE]

        call = multicall.Call()
        for borrower in batch:
            call.add(POOL.getUserAccountData, borrower)

        results = [
            (borrower, result[-1])
            for borrower, result in zip(batch, call())
            if result is not None and result[-1] != MAX_UINT
        ]

        for borrower, health_factor in results:
            context.state.borrowers[borrower].update(
                {
                    "health_factor": str(health_factor),
                    "last_hf_update": current_block,
                }
            )

        updated_count += len(results)

        if results:
            _save_borrowers_db(context.state.borrowers)

    return {
        "updated_count": updated_count,
        "at_risk_checked": len(at_risk_borrowers),
        "safe_checked": len(safe_borrowers),
        "total_checked": len(borrowers_to_check),
    }


@bot.on_startup()
def bot_startup(startup_state: BotState):
    # Load blocks
    last_block = _load_block_db()["last_processed_block"]
    current_block = chain.blocks.head.number

    # Process historical borrow events
    _process_historical_events(last_block, current_block)

    # Update block state
    block_state = {"last_processed_block": current_block}
    _save_block_db(block_state)

    return {
        "message": "Bot started",
        "start_block": last_block,
        "end_block": current_block,
    }


@bot.on_worker_startup()
def worker_startup(state: TaskiqState):
    state.borrowers = _load_borrowers_db()
    state.block_state = _load_block_db()
    return {
        "message": "Worker started",
        "borrowers_count": len(state.borrowers),
        "last_processed_block": state.block_state["last_processed_block"],
    }


@bot.on_(POOL.Borrow)
def handle_borrow(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    *_, health_factor = POOL.getUserAccountData(log.onBehalfOf)

    if health_factor != MAX_UINT:
        context.state.borrowers[log.onBehalfOf] = {
            "health_factor": str(health_factor),
            "last_hf_update": log.block_number,
        }
        _save_borrowers_db(context.state.borrowers)

    return {
        "borrower": log.onBehalfOf,
        "health_factor": health_factor,
        "block_number": log.block_number,
    }


@bot.on_(POOL.Supply)
def handle_supply(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    _update_user_data(log.onBehalfOf, log, context)
    return {"borrower": log.onBehalfOf, "block_number": log.block_number}


@bot.on_(POOL.Repay)
def handle_repay(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    _update_user_data(log.user, log, context)
    return {"borrower": log.user, "block_number": log.block_number}


@bot.on_(POOL.Withdraw)
def handle_withdraw(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    _update_user_data(log.user, log, context)
    return {"borrower": log.user, "block_number": log.block_number}


@bot.on_(chain.blocks)
def exec_block(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
    sync_results = _sync_health_factors(context, block.number)

    context.state.block_state["last_processed_block"] = block.number
    _save_block_db(context.state.block_state)

    return {
        "block_number": block.number,
        "health_factor_updates": sync_results,
    }
