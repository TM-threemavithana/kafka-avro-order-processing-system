from order_pipeline.producer import build_parser, generate_orders


def test_demo_batch_includes_both_failure_paths() -> None:
    orders = generate_orders(
        5,
        seed=42,
        demo=True,
        temporary_failure_product="TEMPORARY_FAILURE",
        permanent_failure_product="PERMANENT_FAILURE",
    )

    assert len(orders) == 5
    assert orders[-2]["product"] == "TEMPORARY_FAILURE"
    assert orders[-1]["product"] == "PERMANENT_FAILURE"
    assert len({order["orderId"] for order in orders}) == 5


def test_seed_is_optional_and_explicit_seed_is_repeatable() -> None:
    assert build_parser().parse_args([]).seed is None

    first = generate_orders(
        3,
        seed=7,
        demo=False,
        temporary_failure_product="TEMPORARY_FAILURE",
        permanent_failure_product="PERMANENT_FAILURE",
    )
    second = generate_orders(
        3,
        seed=7,
        demo=False,
        temporary_failure_product="TEMPORARY_FAILURE",
        permanent_failure_product="PERMANENT_FAILURE",
    )

    comparable = lambda orders: [
        (order["product"], order["price"]) for order in orders
    ]
    assert comparable(first) == comparable(second)
