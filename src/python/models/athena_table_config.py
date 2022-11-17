from dataclasses import dataclass


@dataclass
class AthenaTable:
    name: str
    database: str
    path: str
    parameters: dict = None
    table_type: str = "EXTERNAL_TABLE"
