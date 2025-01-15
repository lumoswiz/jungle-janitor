import os

import click
import numpy as np
import pandas as pd
from ape import Contract
from silverback import SilverbackBot

# Initialize app
bot = SilverbackBot()

# Contracts
POOL_ADDRESSES_PROVIDER = Contract(os.environ["POOL_ADDRESSES_PROVIDER"])
POOL = Contract(POOL_ADDRESSES_PROVIDER.getPool())

# File paths for persistent storage
BORROWERS_FILEPATH = os.environ.get("BORROWERS_FILEPATH", ".db/borrowers.csv")


def _load_borrowers_db() -> pd.DataFrame:
    """Loads borrowers database from persistent storage."""
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
    """Saves borrowers database to persistent storage."""
    os.makedirs(os.path.dirname(BORROWERS_FILEPATH), exist_ok=True)
    db.to_csv(BORROWERS_FILEPATH)
    click.echo(f"Saved borrowers DB with {len(db)} entries")
