import os
from typing import Annotated, Dict

import click
import numpy as np
import pandas as pd
from ape import Contract
from ape.types import ContractLog
from silverback import SilverbackBot
from taskiq import Context, TaskiqDepends, TaskiqState

# Initialize bot
bot = SilverbackBot()

# Contracts
POOL_ADDRESSES_PROVIDER = Contract(os.environ["POOL_ADDRESSES_PROVIDER"])
POOL = Contract(POOL_ADDRESSES_PROVIDER.getPool())

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
