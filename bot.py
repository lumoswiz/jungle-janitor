import os
from typing import Annotated

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


def _load_borrowers_db() -> pd.DataFrame:
    dtype = {
        "health_factor": np.int64,
        "last_hf_update": np.int64,
    }
    return (
        pd.read_csv(BORROWERS_FILEPATH, index_col="address", dtype=dtype)
        if os.path.exists(BORROWERS_FILEPATH)
        else pd.DataFrame(columns=["health_factor", "last_hf_update"])
    )


def _save_borrowers_db(db: pd.DataFrame):
    os.makedirs(os.path.dirname(BORROWERS_FILEPATH), exist_ok=True)
    db.to_csv(BORROWERS_FILEPATH)
    click.echo(f"Saved borrowers DB with {len(db)} entries")


def _load_positions_db() -> pd.DataFrame:
    """Loads positions database from persistent storage."""
    dtype = {
        "debt_assets": object,
        "collateral_assets": object,
        "last_positions_update": np.int64,
    }
    return (
        pd.read_csv(POSITIONS_FILEPATH, index_col="address", dtype=dtype)
        if os.path.exists(POSITIONS_FILEPATH)
        else pd.DataFrame(columns=["debt_assets", "collateral_assets", "last_positions_update"])
    )


def _save_positions_db(db: pd.DataFrame):
    """Saves positions database to persistent storage."""
    os.makedirs(os.path.dirname(POSITIONS_FILEPATH), exist_ok=True)
    db.to_csv(POSITIONS_FILEPATH)
    click.echo(f"Saved positions DB with {len(db)} entries")


@bot.on_worker_startup()
def worker_startup(state: TaskiqState):
    state.borrowers_db = _load_borrowers_db()

    click.echo(f"Worker started with {len(state.borrowers_db)} borrowers in database")
    return {"message": "Worker started", "borrowers_count": len(state.borrowers_db)}


@bot.on_(POOL.Borrow)
def handle_borrow(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    *_, health_factor = POOL.getUserAccountData(log.onBehalfOf)

    if log.onBehalfOf in context.state.borrowers_db.index:
        context.state.borrowers_db.loc[log.onBehalfOf] = {
            "health_factor": health_factor,
            "last_hf_update": log.block_number,
        }
    else:
        context.state.borrowers_db.loc[log.onBehalfOf] = {
            "health_factor": health_factor,
            "last_hf_update": log.block_number,
        }

    _save_borrowers_db(context.state.borrowers_db)

    return {
        "borrower": log.onBehalfOf,
        "health_factor": health_factor,
        "block_number": log.block_number,
    }
