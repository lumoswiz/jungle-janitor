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


def _load_borrowers_db() -> Dict:
    dtype = {
        "borrower_address": str,
        "health_factor": np.int64,
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


def _update_user_data(address, log, context):
    if address in context.state.borrowers:
        health_factor = POOL.getUserAccountData(address)
        if health_factor == 2**256 - 1:
            del context.state.borrowers[address]
            del context.state.positions[address]
        else:
            context.state.borrowers[address].update(
                {"health_factor": health_factor, "last_hf_update": log.block_number}
            )
        _save_borrowers_db(context.state.borrowers)
        _save_positions_db(context.state.positions)


@bot.on_worker_startup()
def worker_startup(state: TaskiqState):
    state.borrowers = _load_borrowers_db()
    state.positions = _load_positions_db()

    click.echo(
        f"Worker started with {len(state.borrowers)} borrowers and {len(state.positions)} positions"
    )
    return {
        "message": "Worker started",
        "borrowers_count": len(state.borrowers),
        "positions_count": len(state.positions),
    }


@bot.on_(POOL.Borrow)
def handle_borrow(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    *_, health_factor = POOL.getUserAccountData(log.onBehalfOf)

    if log.onBehalfOf in context.state.borrowers:
        context.state.borrowers[log.onBehalfOf].update(
            {"health_factor": health_factor, "last_hf_update": log.block_number}
        )
    else:
        context.state.borrowers[log.onBehalfOf] = {
            "health_factor": health_factor,
            "last_hf_update": log.block_number,
        }
        context.state.positions[log.onBehalfOf] = {
            "debt_assets": "",
            "collateral_assets": "",
            "last_positions_update": 0,
        }

    _save_borrowers_db(context.state.borrowers)
    _save_positions_db(context.state.positions)

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
    borrowers_to_check = [
        address
        for address, data in context.state.positions.items()
        if data["last_positions_update"] == 0
    ]

    if borrowers_to_check:
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
            collateral_assets = [
                reserve.underlyingAsset
                for reserve in reserves_data
                if reserve.scaledATokenBalance > 0
            ]

            debt_assets = [
                reserve.underlyingAsset
                for reserve in reserves_data
                if reserve.scaledVariableDebt > 0
            ]

            context.state.positions[borrower].update(
                {
                    "collateral_assets": collateral_assets,
                    "debt_assets": debt_assets,
                    "last_positions_update": block.number,
                }
            )

        _save_positions_db(context.state.positions)

        click.echo(f"Updated positions for {len(results_with_borrowers)} borrowers")

    return {
        "message": "Block execution completed",
        "borrowers_checked": len(borrowers_to_check) if borrowers_to_check else 0,
        "block_number": block.number,
    }
