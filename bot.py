import json
import os
from itertools import product
from pathlib import Path
from typing import Annotated, Any, Dict, List, Tuple

import click
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

abi_path = Path("./abi/flashloan-receiver.json")
with open(abi_path) as f:
    flashloan_receiver_abi = json.load(f)

# Contracts
FLASHLOAN_RECEIVER = Contract(os.environ["FLASHLOAN_RECEIVER"], abi=flashloan_receiver_abi)
POOL_ADDRESSES_PROVIDER = Contract(os.environ["POOL_ADDRESSES_PROVIDER"])
UI_POOL_DATA_PROVIDER_V3 = Contract(os.environ["UI_POOL_DATA_PROVIDER_V3"])
POOL = Contract(POOL_ADDRESSES_PROVIDER.getPool())
POOL_DATA_PROVIDER = Contract(POOL_ADDRESSES_PROVIDER.getPoolDataProvider())
AAVE_ORACLE = Contract(POOL_ADDRESSES_PROVIDER.getPriceOracle())


# File paths
BORROWERS_FILEPATH = os.environ.get("BORROWERS_FILEPATH", ".db/borrowers.csv")
BLOCK_FILEPATH = os.environ.get("BLOCK_FILEPATH", ".db/block.csv")

# Constants
MAX_UINT = 2**256 - 1
START_BLOCK = int(os.environ.get("START_BLOCK", chain.blocks.head.number))
MAX_LIQUIDATION_HF_THRESHOLD = 0.95 * 10**18
LIQUIDATION_HF_THRESHOLD = 1 * 10**18
AT_RISK_HF_THRESHOLD = 1.5 * 10**18
AT_RISK_BLOCK_CHECK = 480
REGULAR_BLOCK_CHECK = 3600
MULTICALL_BATCH_SIZE = 50
MAX_CLOSE_FACTOR = 10000
DEFAULT_CLOSE_FACTOR = 5000
NATIVE_ASSET_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
PRICE_ONE = 100000000
HALF_BPS = 5000
BASE_BPS = 10000

# Auto sign
PROMPT_AUTOSIGN = bot.signer is not None


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
        if health_factor == MAX_UINT:
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
    borrowers_to_remove = []

    for i in range(0, len(borrowers_to_check), MULTICALL_BATCH_SIZE):
        batch = borrowers_to_check[i : i + MULTICALL_BATCH_SIZE]

        call = multicall.Call()
        for borrower in batch:
            call.add(POOL.getUserAccountData, borrower)

        for borrower, result in zip(batch, call()):
            if result is None:
                continue

            health_factor = result[-1]
            if health_factor == MAX_UINT:
                borrowers_to_remove.append(borrower)
            else:
                context.state.borrowers[borrower].update(
                    {
                        "health_factor": str(health_factor),
                        "last_hf_update": current_block,
                    }
                )
                updated_count += 1

        for borrower in borrowers_to_remove:
            del context.state.borrowers[borrower]

        if updated_count > 0 or borrowers_to_remove:
            _save_borrowers_db(context.state.borrowers)

    return {
        "updated_count": updated_count,
        "removed_count": len(borrowers_to_remove),
        "at_risk_checked": len(at_risk_borrowers),
        "safe_checked": len(safe_borrowers),
        "total_checked": len(borrowers_to_check),
    }


def _identify_liquidatable_borrowers(borrowers: Dict) -> List[str]:
    return [
        address
        for address, data in borrowers.items()
        if int(data["health_factor"]) < LIQUIDATION_HF_THRESHOLD
    ]


def _parse_user_reserves_data(reserves_data: Any) -> Dict[str, List[str]]:
    collateral_positions = [
        reserve.underlyingAsset
        for reserve in reserves_data
        if reserve.scaledATokenBalance > 0 and reserve.usageAsCollateralEnabled
    ]

    debt_positions = [
        reserve.underlyingAsset for reserve in reserves_data if reserve.scaledVariableDebt > 0
    ]

    return {
        "collateral": collateral_positions,
        "debt": debt_positions,
    }


def _get_all_reserves() -> List[str]:
    reserves = POOL_DATA_PROVIDER.getAllReservesTokens()
    return [reserve.tokenAddress for reserve in reserves]


def _get_reserve_configurations(reserve_addresses: List[str]) -> List[Tuple[int, int]]:
    call = multicall.Call()

    for address in reserve_addresses:
        call.add(POOL_DATA_PROVIDER.getReserveConfigurationData, address)

    results = []
    for result in call():
        if result is not None:
            decimals, _, _, liquidation_bonus, *_ = result
            results.append((int(decimals), int(liquidation_bonus)))

    return results


def _update_reserve_configs() -> Dict[str, Dict]:
    reserve_addresses = _get_all_reserves()

    configurations = _get_reserve_configurations(reserve_addresses)
    current_block = chain.blocks.head.number

    reserve_configs = {}
    for i, address in enumerate(reserve_addresses):
        decimals, liquidation_bonus = configurations[i]

        reserve_configs[address] = {
            "decimals": decimals,
            "liquidation_bonus": liquidation_bonus,
            "last_update_block": current_block,
        }

    return reserve_configs


def _get_reserve_prices(reserve_addresses: List[str]) -> List[int]:
    return AAVE_ORACLE.getAssetsPrices(reserve_addresses)


def _get_user_reserve_data(user_address: str, reserve_addresses: List[str]) -> Dict[str, Dict]:
    call = multicall.Call()

    for address in reserve_addresses:
        call.add(POOL_DATA_PROVIDER.getUserReserveData, address, user_address)

    user_reserve_data = {}
    for address, result in zip(reserve_addresses, call()):
        if result is not None:
            atoken_balance, _, variable_debt, *_ = result

            user_reserve_data[address] = {
                "atoken_balance": int(atoken_balance),
                "variable_debt": int(variable_debt),
            }

    return user_reserve_data


def _get_liquidatable_data(borrowers_to_check: List[str], context: Context) -> Dict:
    if not borrowers_to_check:
        return {}

    liquidatable_data = {}

    for i in range(0, len(borrowers_to_check), MULTICALL_BATCH_SIZE):
        batch = borrowers_to_check[i : i + MULTICALL_BATCH_SIZE]

        call = multicall.Call()
        for borrower in batch:
            args = (POOL_ADDRESSES_PROVIDER, borrower)
            call.add(UI_POOL_DATA_PROVIDER_V3.getUserReservesData, *args)

        results = [
            (borrower, result) for borrower, result in zip(batch, call()) if result is not None
        ]

        for borrower, (reserves_data, *_) in results:
            positions = _parse_user_reserves_data(reserves_data)
            health_factor = int(context.state.borrowers[borrower]["health_factor"])

            liquidatable_data[borrower] = {
                "collateral": positions["collateral"],
                "debt": positions["debt"],
                "can_be_max_liquidated": health_factor < MAX_LIQUIDATION_HF_THRESHOLD,
            }

    return liquidatable_data


def _build_liquidation_state(
    liquidatable_borrowers: List[str],
    reserve_prices: Dict[str, int],
    reserve_configs: Dict[str, Dict],
    context: Context,
) -> Dict[str, Dict]:
    liquidatable_data = _get_liquidatable_data(liquidatable_borrowers, context)
    if not liquidatable_data:
        return {}

    liquidation_state = {}

    for borrower, positions in liquidatable_data.items():
        reserve_addresses = positions["collateral"] + positions["debt"]
        user_reserves = _get_user_reserve_data(borrower, reserve_addresses)

        collateral_state = {
            reserve: {
                "decimals": reserve_configs[reserve]["decimals"],
                "liquidation_bonus": reserve_configs[reserve]["liquidation_bonus"],
                "price": reserve_prices[reserve],
                "balance": user_reserves[reserve]["atoken_balance"],
            }
            for reserve in positions["collateral"]
        }

        debt_state = {
            reserve: {
                "decimals": reserve_configs[reserve]["decimals"],
                "price": reserve_prices[reserve],
                "amount": user_reserves[reserve]["variable_debt"],
            }
            for reserve in positions["debt"]
        }

        liquidation_state[borrower] = {
            "collateral": collateral_state,
            "debt": debt_state,
            "can_be_max_liquidated": positions["can_be_max_liquidated"],
        }

    return liquidation_state


def _percent_mul(value: int, bps: int) -> int:
    return (HALF_BPS + value * bps) // BASE_BPS


def _percent_div(value: int, bps: int) -> int:
    half_bps = bps // 2
    return (half_bps + value * BASE_BPS) // bps


def _calculate_liquidation_amounts_base(
    borrower_state: Dict[str, Dict], collateral_addr: str, debt_addr: str
) -> Tuple[int, int]:
    collateral = borrower_state["collateral"][collateral_addr]
    debt = borrower_state["debt"][debt_addr]

    debt_amount = debt["amount"]
    debt_to_cover = (
        debt_amount
        if borrower_state["can_be_max_liquidated"]
        else (debt_amount * DEFAULT_CLOSE_FACTOR) // MAX_CLOSE_FACTOR
    )

    collateral_unit = 10 ** collateral["decimals"]
    debt_unit = 10 ** debt["decimals"]

    base_collateral = (debt["price"] * debt_to_cover * collateral_unit) // (
        collateral["price"] * debt_unit
    )

    collateral_to_liquidate = _percent_mul(base_collateral, collateral["liquidation_bonus"])

    if collateral_to_liquidate > collateral["balance"]:
        collateral_to_liquidate = collateral["balance"]
        debt_to_cover = (collateral["price"] * collateral_to_liquidate * debt_unit) // _percent_div(
            debt["price"] * collateral_unit, collateral["liquidation_bonus"]
        )

    return collateral_to_liquidate, debt_to_cover


def _calculate_liquidation_amounts(
    borrower_state: Dict[str, Dict], collateral_addr: str, debt_addr: str, native_price: int
) -> Tuple[int, int]:
    collateral_to_liquidate, debt_to_cover = _calculate_liquidation_amounts_base(
        borrower_state, collateral_addr, debt_addr
    )

    collateral_price = borrower_state["collateral"][collateral_addr]["price"]
    collateral_unit = 10 ** borrower_state["collateral"][collateral_addr]["decimals"]
    native_unit = 10**18

    collateral_to_liquidate_native = (
        collateral_to_liquidate
        if collateral_addr == NATIVE_ASSET_ADDRESS
        else (collateral_to_liquidate * collateral_price * native_unit)
        // (native_price * collateral_unit)
    )

    return collateral_to_liquidate_native, debt_to_cover


def _find_optimal_liquidation_pairs(
    liquidation_state: Dict[str, Dict], native_price: int
) -> Dict[str, Dict]:
    optimal_pairs = {}

    for borrower, state in liquidation_state.items():
        max_value = 0
        best_pair = None

        for collateral_addr, debt_addr in product(state["collateral"], state["debt"]):
            collateral_to_liquidate_native, debt_to_cover = _calculate_liquidation_amounts(
                state, collateral_addr, debt_addr, native_price
            )

            if collateral_to_liquidate_native > max_value:
                max_value = collateral_to_liquidate_native
                best_pair = {
                    "collateral": collateral_addr,
                    "debt": debt_addr,
                    "collateral_to_liquidate_native": collateral_to_liquidate_native,
                    "debt_to_cover": debt_to_cover,
                }

        if best_pair:
            optimal_pairs[borrower] = best_pair

    return optimal_pairs


def _execute_liquidations(optimal_pairs: Dict[str, Dict], context: Context) -> Dict:
    if not optimal_pairs:
        return {"liquidations_attempted": 0, "liquidations_executed": 0}

    sorted_pairs = sorted(
        optimal_pairs.items(), key=lambda x: x[1]["collateral_to_liquidate_native"], reverse=True
    )

    execution_stats = {"liquidations_attempted": len(sorted_pairs), "liquidations_executed": 0}

    if not bot.signer:
        click.echo(
            f"Would have executed {len(sorted_pairs)} liquidations "
            f"(sorted by value: {[p[1]['collateral_to_liquidate_native'] for p in sorted_pairs]}) "
            f"but no signer configured."
        )
        return execution_stats

    for borrower, pair in sorted_pairs:
        try:
            click.echo(
                f"Attempting liquidation for borrower {borrower} "
                f"with value {pair['collateral_to_liquidate_native']}"
            )

            FLASHLOAN_RECEIVER.requestFlashLoan(
                pair["collateral"],
                pair["debt"],
                borrower,
                pair["debt_to_cover"],
                sender=bot.signer,
            )

            execution_stats["liquidations_executed"] += 1
            click.echo(f"Successfully liquidated position for borrower {borrower}")

        except Exception as e:
            click.secho(f"Failed to liquidate borrower {borrower}: {str(e)}", fg="red", bold=True)
            continue

    return execution_stats


def _process_liquidations(context: Context) -> Dict:
    liquidatable_borrowers = _identify_liquidatable_borrowers(context.state.borrowers)
    if not liquidatable_borrowers:
        return {"liquidations_processed": 0}

    all_reserves = list(bot.state.reserve_configs.keys())
    current_prices = _get_reserve_prices(all_reserves)
    reserve_prices = dict(zip(all_reserves, current_prices))

    liquidation_state = _build_liquidation_state(
        liquidatable_borrowers, reserve_prices, bot.state.reserve_configs, context
    )

    optimal_pairs = _find_optimal_liquidation_pairs(
        liquidation_state, reserve_prices[NATIVE_ASSET_ADDRESS]
    )

    execution_results = _execute_liquidations(optimal_pairs, context)

    return {
        "liquidatable_borrowers": len(liquidatable_borrowers),
        "positions_processed": len(liquidation_state),
        "optimal_pairs_found": len(optimal_pairs),
        "liquidations_attempted": execution_results["liquidations_attempted"],
        "liquidations_executed": execution_results["liquidations_executed"],
    }


@bot.on_startup()
def bot_startup(startup_state: BotState):
    if PROMPT_AUTOSIGN and click.confirm("Enable autosign?"):
        bot.signer.set_autosign(enabled=True)
        click.echo(f"Autosign enabled for account: {bot.signer.address}")

    # Initialize reserve configs
    bot.state.reserve_configs = _update_reserve_configs()

    # Load blocks
    last_block = _load_block_db()["last_processed_block"]
    current_block = chain.blocks.head.number

    # Process historical borrow events
    _process_historical_events(last_block, current_block)

    # Update block state
    block_state = {"last_processed_block": current_block}
    _save_block_db(block_state)

    return {"message": "Bot started", "start_block": last_block, "end_block": current_block}


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
    # Update health factors first
    sync_results = _sync_health_factors(context, block.number)

    # Process any liquidations
    liquidation_results = _process_liquidations(context)
    click.echo(f"Liquidation results: {liquidation_results}")

    # Update block state
    context.state.block_state["last_processed_block"] = block.number
    _save_block_db(context.state.block_state)

    return {
        "block_number": block.number,
        "health_factor_updates": sync_results,
        "liquidation_results": liquidation_results,
    }
