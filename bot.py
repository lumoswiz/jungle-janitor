import os

from ape import Contract
from silverback import SilverbackBot

# Initialize bot
bot = SilverbackBot()

# Pool Addresses Provider contract
POOL_ADDRESSES_PROVIDER = Contract(os.environ["POOL_ADDRESSES_PROVIDER"])

# Pool contract
POOL = Contract(POOL_ADDRESSES_PROVIDER.getPool())
