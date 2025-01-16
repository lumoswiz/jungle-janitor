import os
from typing import Annotated, Dict

import click
import numpy as np
import pandas as pd
from ape import Contract, chain
from ape.api import BlockAPI
from ape.types import ContractLog
from ape_ethereum import multicall
from silverback import SilverbackBot
from taskiq import Context, TaskiqDepends, TaskiqState

# Initialize bot
bot = SilverbackBot()

# Contracts
POOL_ADDRESSES_PROVIDER = Contract(os.environ["POOL_ADDRESSES_PROVIDER"])
POOL = Contract(POOL_ADDRESSES_PROVIDER.getPool())
UI_POOL_DATA_PROVIDER_V3 = Contract(os.environ["UI_POOL_DATA_PROVIDER_V3"])

# File paths for persistent storage
BORROWERS_FILEPATH = os.environ.get("BORROWERS_FILEPATH", ".db/borrowers.csv")
POSITIONS_FILEPATH = os.environ.get("POSITIONS_FILEPATH", ".db/positions.csv")
BLOCK_FILEPATH = os.environ.get("BLOCK_FILEPATH", ".db/block.csv")

# Environment variables
START_BLOCK = int(os.environ.get("START_BLOCK", chain.blocks.head.number))

# Constants
MAX_UINT = 2**256 - 1


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


def _load_positions_db() -> Dict:
    dtype = {
        "borrower_address": str,
        "debt_assets": object,
        "collateral_assets": object,
        "last_positions_update": np.int64,
    }
    df = (
        pd.read_csv(POSITIONS_FILEPATH, dtype=dtype)
        if os.path.exists(POSITIONS_FILEPATH)
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
    click.echo(f"Saved borrowers DB with {len(data)} entries")


def _save_positions_db(data: Dict):
    os.makedirs(os.path.dirname(POSITIONS_FILEPATH), exist_ok=True)
    df = pd.DataFrame.from_dict(data, orient="index").reset_index()
    df.columns = ["borrower_address", "debt_assets", "collateral_assets", "last_positions_update"]
    df.to_csv(POSITIONS_FILEPATH, index=False)
    click.echo(f"Saved positions DB with {len(data)} entries")


def _save_block_db(data: Dict):
    os.makedirs(os.path.dirname(BLOCK_FILEPATH), exist_ok=True)
    df = pd.DataFrame([data])
    df.to_csv(BLOCK_FILEPATH, index=False)
    click.echo(f"Saved block state: {data}")


def _update_user_data(address, log, context):
    if address in context.state.borrowers:
        *_, health_factor = POOL.getUserAccountData(address)
        if health_factor == 2**256 - 1:
            del context.state.borrowers[address]
            del context.state.positions[address]
        else:
            context.state.borrowers[address].update(
                {"health_factor": health_factor, "last_hf_update": log.block_number}
            )
        _save_borrowers_db(context.state.borrowers)
        _save_positions_db(context.state.positions)


def _update_borrower_positions(borrower: str, reserves_data) -> dict:
    collateral_assets = [
        reserve.underlyingAsset for reserve in reserves_data if reserve.scaledATokenBalance > 0
    ]

    debt_assets = [
        reserve.underlyingAsset for reserve in reserves_data if reserve.scaledVariableDebt > 0
    ]

    return {
        "collateral_assets": collateral_assets,
        "debt_assets": debt_assets,
    }


def _update_block_state(block_number: int, context: Context):
    if not hasattr(context.state, "block_state"):
        context.state.block_state = _load_block_db()

    context.state.block_state = {"last_processed_block": block_number}
    _save_block_db(context.state.block_state)


def _process_pending_borrowers(context: Context, block_number: int) -> tuple[int, list]:
    borrowers_to_check = [
        address
        for address, data in context.state.positions.items()
        if data["last_positions_update"] == 0
    ]

    if not borrowers_to_check:
        return 0, []

    call = multicall.Call()
    for borrower in borrowers_to_check:
        args = (POOL_ADDRESSES_PROVIDER, borrower)
        call.add(UI_POOL_DATA_PROVIDER_V3.getUserReservesData, *args)

    results_with_borrowers = [
        (borrower, result)
        for borrower, result in zip(borrowers_to_check, call())
        if result is not None
    ]

    for borrower, (reserves_data, _) in results_with_borrowers:
        position_data = _update_borrower_positions(borrower, reserves_data)
        context.state.positions[borrower].update(
            {
                **position_data,
                "last_positions_update": block_number,
            }
        )

    if results_with_borrowers:
        _save_positions_db(context.state.positions)
        click.echo(f"Updated positions for {len(results_with_borrowers)} borrowers")

    return len(results_with_borrowers), borrowers_to_check


def initialize_new_borrower(
    borrower: str, health_factor: int, block_number: int, borrowers: Dict, positions: Dict
) -> None:
    borrowers[borrower] = {"health_factor": health_factor, "last_hf_update": block_number}
    positions[borrower] = {"debt_assets": "", "collateral_assets": "", "last_positions_update": 0}


def update_borrower_health_factor(
    borrower: str, health_factor: int, block_number: int, borrowers: Dict
) -> None:
    borrowers[borrower].update({"health_factor": health_factor, "last_hf_update": block_number})


@bot.on_worker_startup()
def worker_startup(state: TaskiqState):
    state.borrowers = _load_borrowers_db()
    state.positions = _load_positions_db()
    state.block_state = _load_block_db()

    click.echo(
        f"Worker started:\n"
        f"  Borrowers: {len(state.borrowers)}\n"
        f"  Positions: {len(state.positions)}\n"
        f"  Last block: {state.block_state['last_processed_block']}"
    )
    return {
        "message": "Worker started",
        "borrowers_count": len(state.borrowers),
        "positions_count": len(state.positions),
        "last_processed_block": state.block_state["last_processed_block"],
    }


def handle_borrow(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    *_, health_factor = POOL.getUserAccountData(log.onBehalfOf)

    if log.onBehalfOf in context.state.borrowers:
        update_borrower_health_factor(
            borrower=log.onBehalfOf,
            health_factor=health_factor,
            block_number=log.block_number,
            borrowers=context.state.borrowers,
        )
    else:
        initialize_new_borrower(
            borrower=log.onBehalfOf,
            health_factor=health_factor,
            block_number=log.block_number,
            borrowers=context.state.borrowers,
            positions=context.state.positions,
        )

    _save_borrowers_db(context.state.borrowers)
    _save_positions_db(context.state.positions)
    _update_block_state(log.block_number, context)

    return {
        "borrower": log.onBehalfOf,
        "health_factor": health_factor,
        "block_number": log.block_number,
    }


@bot.on_(POOL.Supply)
def handle_supply(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    _update_user_data(log.onBehalfOf, log, context)
    _update_block_state(log.block_number, context)
    return {"borrower": log.onBehalfOf, "block_number": log.block_number}


@bot.on_(POOL.Repay)
def handle_repay(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    _update_user_data(log.user, log, context)
    _update_block_state(log.block_number, context)
    return {"borrower": log.user, "block_number": log.block_number}


@bot.on_(POOL.Withdraw)
def handle_withdraw(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    _update_user_data(log.user, log, context)
    _update_block_state(log.block_number, context)
    return {"borrower": log.user, "block_number": log.block_number}


@bot.on_(chain.blocks)
def exec_block(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
    updated_count, borrowers_checked = _process_pending_borrowers(context, block.number)

    return {
        "message": "Block execution completed",
        "borrowers_checked": len(borrowers_checked),
        "borrowers_updated": updated_count,
        "block_number": block.number,
    }
