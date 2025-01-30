from typing import Any, Dict, List, TypedDict

import pytest


@pytest.fixture
def liquidation_constants():
    return {
        "MAX_LIQUIDATION_HF_THRESHOLD": int(0.95 * 10**18),
        "LIQUIDATION_HF_THRESHOLD": int(1 * 10**18),
        "AT_RISK_HF_THRESHOLD": int(1.5 * 10**18),
        "PRICE_ONE": 100000000,
        "HALF_BPS": 5000,
        "BASE_BPS": 10000,
        "MAX_CLOSE_FACTOR": 10000,
        "DEFAULT_CLOSE_FACTOR": 5000,
    }


class ReserveData(TypedDict):
    underlyingAsset: str
    scaledATokenBalance: int
    usageAsCollateralEnabled: bool
    scaledVariableDebt: int


@pytest.fixture
def mock_reserves_data() -> Dict[str, ReserveData]:
    return {
        "0x782dF99676f014b6fD6000626073c78DbA205D2E": [
            {
                "underlyingAsset": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
                "scaledATokenBalance": 976395252,
                "usageAsCollateralEnabled": True,
                "scaledVariableDebt": 0,
            },
            {
                "underlyingAsset": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "scaledATokenBalance": 50727847,
                "usageAsCollateralEnabled": True,
                "scaledVariableDebt": 0,
            },
            {
                "underlyingAsset": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "scaledATokenBalance": 0,
                "usageAsCollateralEnabled": False,
                "scaledVariableDebt": 240009265489591162,
            },
        ]
    }


@pytest.fixture
def mock_health_factors() -> Dict[str, int]:
    return {
        "0x782dF99676f014b6fD6000626073c78DbA205D2E": 0.9 * 10**18,
    }


@pytest.fixture
def mock_reserve_configs() -> Dict[str, Dict[str, Any]]:
    return {
        "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": {
            "decimals": 6,
            "liquidation_bonus": 10500,
        },
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": {"decimals": 6, "liquidation_bonus": 10500},
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": {
            "decimals": 18,
            "liquidation_bonus": 10500,
        },
    }


@pytest.fixture
def mock_prices() -> Dict[str, int]:
    return {
        "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": 99986000,
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": 99996893,
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 321443000000,
    }


@pytest.fixture
def mock_borrower_state(
    mock_reserves_data: Dict[str, List[ReserveData]],
    mock_health_factors: Dict[str, int],
    mock_reserve_configs: Dict[str, Dict[str, Any]],
    mock_prices: Dict[str, int],
    liquidation_constants: Dict[str, int],
) -> Dict[str, Dict[str, Any]]:
    borrower_state = {}

    for borrower, reserves in mock_reserves_data.items():
        collateral = {}
        debt = {}

        for reserve in reserves:
            asset = reserve["underlyingAsset"]

            if reserve["usageAsCollateralEnabled"] and reserve["scaledATokenBalance"] > 0:
                collateral[asset] = {
                    "decimals": mock_reserve_configs[asset]["decimals"],
                    "liquidation_bonus": mock_reserve_configs[asset]["liquidation_bonus"],
                    "price": mock_prices[asset],
                    "balance": reserve["scaledATokenBalance"],
                }

            if reserve["scaledVariableDebt"] > 0:
                debt[asset] = {
                    "decimals": mock_reserve_configs[asset]["decimals"],
                    "price": mock_prices[asset],
                    "amount": reserve["scaledVariableDebt"],
                }

        health_factor = mock_health_factors[borrower]
        can_be_max_liquidated = (
            health_factor < liquidation_constants["MAX_LIQUIDATION_HF_THRESHOLD"]
        )

        borrower_state[borrower] = {
            "collateral": collateral,
            "debt": debt,
            "can_be_max_liquidated": can_be_max_liquidated,
        }

    return borrower_state
