from order_pipeline.producer import generate_orders


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
