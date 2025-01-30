from typing import Dict, List, Tuple

###############################################################################
#                              Helper Functions                                 #
###############################################################################


def _percent_mul(value: int, bps: int, constants: Dict[str, int]) -> int:
    return (constants["HALF_BPS"] + value * bps) // constants["BASE_BPS"]


def _percent_div(value: int, bps: int, constants: Dict[str, int]) -> int:
    half_bps = bps // 2
    return (half_bps + value * constants["BASE_BPS"]) // bps


###############################################################################
#                            Liquidatable Borrowers                           #
###############################################################################


def identify_liquidatable_borrowers_test(
    borrowers: Dict[str, int], liquidation_threshold: int
) -> List[str]:
    """Test version of _identify_liquidatable_borrowers that takes direct inputs"""
    return [
        address
        for address, health_factor in borrowers.items()
        if health_factor < liquidation_threshold
    ]


def test_identify_liquidatable_borrowers(
    mock_health_factors: Dict[str, int], liquidation_constants: Dict[str, int]
):
    liquidatable_borrowers = identify_liquidatable_borrowers_test(
        mock_health_factors, liquidation_constants["LIQUIDATION_HF_THRESHOLD"]
    )

    assert len(liquidatable_borrowers) == 1
    assert "0x782dF99676f014b6fD6000626073c78DbA205D2E" in liquidatable_borrowers


###############################################################################
#                           Liquidation Calculations                          #
###############################################################################


def calculate_liquidation_amounts_base_test(
    borrower_state: Dict[str, Dict],
    collateral_addr: str,
    debt_addr: str,
    constants: Dict[str, int],
) -> Tuple[int, int]:
    """Test version of _calculate_liquidation_amounts_base"""
    collateral = borrower_state["collateral"][collateral_addr]
    debt = borrower_state["debt"][debt_addr]

    debt_amount = debt["amount"]
    debt_to_cover = (
        debt_amount
        if borrower_state["can_be_max_liquidated"]
        else (debt_amount * constants["DEFAULT_CLOSE_FACTOR"]) // constants["MAX_CLOSE_FACTOR"]
    )

    collateral_unit = 10 ** collateral["decimals"]
    debt_unit = 10 ** debt["decimals"]

    base_collateral = (debt["price"] * debt_to_cover * collateral_unit) // (
        collateral["price"] * debt_unit
    )

    collateral_to_liquidate = _percent_mul(
        base_collateral, collateral["liquidation_bonus"], constants
    )

    if collateral_to_liquidate > collateral["balance"]:
        collateral_to_liquidate = collateral["balance"]
        debt_to_cover = (collateral["price"] * collateral_to_liquidate * debt_unit) // _percent_div(
            debt["price"] * collateral_unit, collateral["liquidation_bonus"], constants
        )

    return collateral_to_liquidate, debt_to_cover


def calculate_liquidation_amounts_test(
    borrower_state: Dict[str, Dict],
    collateral_addr: str,
    debt_addr: str,
    native_price: int,
    native_asset_address: str,
    constants: Dict[str, int],
) -> Tuple[int, int]:
    """Test version of _calculate_liquidation_amounts"""
    collateral_to_liquidate, debt_to_cover = calculate_liquidation_amounts_base_test(
        borrower_state, collateral_addr, debt_addr, constants
    )

    collateral_price = borrower_state["collateral"][collateral_addr]["price"]
    collateral_unit = 10 ** borrower_state["collateral"][collateral_addr]["decimals"]
    native_unit = 10**18

    collateral_to_liquidate_native = (
        collateral_to_liquidate
        if collateral_addr == native_asset_address
        else (collateral_to_liquidate * collateral_price * native_unit)
        // (native_price * collateral_unit)
    )

    return collateral_to_liquidate_native, debt_to_cover


###############################################################################
#                           Optimal Pair Selection                            #
###############################################################################


def find_optimal_liquidation_pairs_test(
    liquidation_state: Dict[str, Dict],
    native_price: int,
    native_asset_address: str,
    constants: Dict[str, int],
) -> Dict[str, Dict]:
    """Test version of _find_optimal_liquidation_pairs"""
    optimal_pairs = {}

    for borrower, state in liquidation_state.items():
        max_value = 0
        best_pair = None

        for collateral_addr in state["collateral"]:
            for debt_addr in state["debt"]:
                collateral_to_liquidate_native, debt_to_cover = calculate_liquidation_amounts_test(
                    state,
                    collateral_addr,
                    debt_addr,
                    native_price,
                    native_asset_address,
                    constants,
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


def test_find_optimal_liquidation_pairs(mock_borrower_state, mock_prices, liquidation_constants):
    """Test optimal liquidation pair finding"""
    native_asset = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
    print(f"\nNative Asset Price: {mock_prices[native_asset]}")

    print("\nInitial Borrower States:")
    for borrower, state in mock_borrower_state.items():
        print(f"\nBorrower: {borrower}")
        print("Collateral Positions:")
        for addr, details in state["collateral"].items():
            print(f"  Address: {addr}")
            print(f"  Details: {details}")
        print("Debt Positions:")
        for addr, details in state["debt"].items():
            print(f"  Address: {addr}")
            print(f"  Details: {details}")

    optimal_pairs = find_optimal_liquidation_pairs_test(
        mock_borrower_state,
        mock_prices[native_asset],
        native_asset,
        liquidation_constants,
    )

    assert len(optimal_pairs) > 0
    print(f"\nFound {len(optimal_pairs)} optimal liquidation pairs")

    for borrower, pair in optimal_pairs.items():
        print(f"\nOptimal Pair for Borrower {borrower}:")
        print(f"  Collateral Address: {pair['collateral']}")
        print(f"  Debt Address: {pair['debt']}")
        print(f"  Collateral Value (in native): {pair['collateral_to_liquidate_native']}")
        print(f"  Debt to Cover: {pair['debt_to_cover']}")

        state = mock_borrower_state[borrower]
        max_value = pair["collateral_to_liquidate_native"]

        print("\nChecking all pairs:")
        for c_addr in state["collateral"]:
            for d_addr in state["debt"]:
                value, debt = calculate_liquidation_amounts_test(
                    state,
                    c_addr,
                    d_addr,
                    mock_prices[native_asset],
                    native_asset,
                    liquidation_constants,
                )
                print(f"  Pair ({c_addr}, {d_addr}):")
                print(f"    Value in native: {value}")
                print(f"    Debt to cover: {debt}")
                assert value <= max_value, "Found a better liquidation pair than the optimal one"
