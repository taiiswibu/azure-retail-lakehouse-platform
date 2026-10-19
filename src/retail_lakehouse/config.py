from __future__ import annotations

import re
from dataclasses import dataclass

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
AZURE_RESOURCE_ID = re.compile(
    r"^/subscriptions/[0-9a-fA-F-]+/resourceGroups/[^/]+/providers/"
    r"Microsoft\.Databricks/accessConnectors/[^/]+$"
)
STORAGE_ACCOUNT = re.compile(r"^[a-z0-9]{3,24}$")


def require_identifier(value: str, label: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} must contain only letters, numbers, and underscores: {value!r}")
    return value


def require_storage_account(value: str) -> str:
    if not STORAGE_ACCOUNT.fullmatch(value):
        raise ValueError("Storage account must be 3-24 lowercase alphanumeric characters")
    return value


def require_access_connector_id(value: str) -> str:
    if not AZURE_RESOURCE_ID.fullmatch(value):
        raise ValueError("Access connector resource ID has an unexpected format")
    return value


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str
    secret_scope: str = "retail-secrets"
    eventhubs_secret_key: str = "eventhubs-connection-string"

    def __post_init__(self) -> None:
        require_identifier(self.catalog, "catalog")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", self.secret_scope):
            raise ValueError("secret_scope contains unsupported characters")

    def table(self, layer: str, name: str) -> str:
        require_identifier(layer, "layer")
        require_identifier(name, "table")
        return f"{self.catalog}.{layer}.{name}"

    @property
    def landing_volume(self) -> str:
        return f"/Volumes/{self.catalog}/landing/files"

    @property
    def state_volume(self) -> str:
        return f"/Volumes/{self.catalog}/checkpoints/state"

