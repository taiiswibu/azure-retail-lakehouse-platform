from __future__ import annotations

import pytest

from retail_lakehouse.config import LakehouseConfig, require_storage_account


def test_config_builds_three_part_table_name() -> None:
    config = LakehouseConfig("retail_dev")
    assert config.table("silver", "orders") == "retail_dev.silver.orders"
    assert config.landing_volume == "/Volumes/retail_dev/landing/files"


@pytest.mark.parametrize("value", ["bad-name", "1catalog", "retail dev", "retail.dev"])
def test_catalog_rejects_unsafe_identifiers(value: str) -> None:
    with pytest.raises(ValueError):
        LakehouseConfig(value)


@pytest.mark.parametrize("value", ["UPPERCASE", "ab", "storage_with_underscore"])
def test_storage_account_validation(value: str) -> None:
    with pytest.raises(ValueError):
        require_storage_account(value)

