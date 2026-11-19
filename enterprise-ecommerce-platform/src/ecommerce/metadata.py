
# src/ecommerce/metadata.py

from dataclasses import dataclass
from pathlib import Path
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, TimestampType,
    DateType, LongType, DecimalType
)
import yaml
import re


def parse_spark_type(dtype):
    """Convert YAML datatype into PySpark datatype."""

    types = {
        "string": StringType(),
        "int": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "long": LongType()
    }

    dtype = dtype.strip().lower()

    if dtype in types:
        return types[dtype]

    match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", dtype)

    if match:
        precision, scale = map(int, match.groups())
        if not (1 <= precision <= 38 and 0 <= scale <= precision):
            raise ValueError(f"Invalid decimal type: {dtype}")
        return DecimalType(precision, scale)

    raise ValueError(f"Unsupported datatype: {dtype}")


@dataclass(frozen=True)
class DatasetConfig:
    """Dataset metadata and generated paths."""

    name: str
    metadata: dict
    config: dict

    @property
    def full_bronze_table(self):
        p = self.config["platform"]
        return f'{p["catalog"]}.{p["schemas"]["bronze"]}.{self.metadata["bronze_table"]}'

    @property
    def full_silver_table(self):
        p = self.config["platform"]
        return f'{p["catalog"]}.{p["schemas"]["silver"]}.{self.metadata["silver_table"]}'

    @property
    def source_file_path(self):
        root = self.config["platform"]["volumes"]["raw_source_root"]
        return f'{root.rstrip("/")}/{self.metadata["file_pattern"]}'

    @property
    def checkpoint_location(self):
        root = self.config["platform"]["volumes"]["streaming_state_root"]
        folder = self.config["defaults"]["ingestion"]["checkpoint_directory"]
        return f'{root.rstrip("/")}/{folder}/{self.metadata["source_id"]}'

    @property
    def schema_location(self):
        root = self.config["platform"]["volumes"]["streaming_state_root"]
        folder = self.config["defaults"]["ingestion"]["schema_directory"]
        return f'{root.rstrip("/")}/{folder}/{self.metadata["source_id"]}'

    @property
    def target_struct_type(self):
        return StructType([
            StructField(
                col["name"],
                parse_spark_type(col["type"]),
                col.get("nullable", True)
            )
            for col in self.metadata["columns"]
        ])

    @property
    def bronze_struct_type(self):
        return StructType([
            StructField(col["name"], StringType(), True)
            for col in self.metadata["columns"]
        ])

    @property
    def csv_options(self):
        return {
            **self.config["defaults"]["csv_options"],
            **self.metadata.get("csv_options", {})
        }

    @property
    def autoloader_options(self):
        options = {
            **self.config["defaults"]["autoloader_options"],
            **self.metadata.get("autoloader_options", {})
        }
        options["pathGlobFilter"] = self.metadata["file_pattern"]
        return options

    @property
    def load_type(self):
        return self.metadata["load_type"]

    @property
    def business_keys(self):
        return tuple(self.metadata["business_keys"])

    @property
    def quality_rules(self):
        return self.metadata.get("quality_rules", {})


class MetadataRegistry:
    """Load and validate dataset configurations from dev.yml."""

    def __init__(self, config):
        self.config = config
        self.environment = config["environment"]
        self.catalog = config["platform"]["catalog"]

        self._datasets = {}

        for name, metadata in config["datasets"].items():

            columns = [c["name"] for c in metadata["columns"]]
            keys = metadata["business_keys"]

            if len(columns) != len(set(columns)):
                raise ValueError(f"Duplicate columns: {name}")

            if not keys or any(k not in columns for k in keys):
                raise ValueError(f"Invalid business keys: {name}")

            for col in metadata["columns"]:
                parse_spark_type(col["type"])

            if metadata["load_type"] == "scd2":
                tracked = metadata.get("tracked_columns", [])
                if not tracked or any(c not in columns for c in tracked):
                    raise ValueError(f"Invalid SCD2 columns: {name}")

            self._datasets[name] = DatasetConfig(
                name, metadata, config
            )

        source_ids = [
            ds.metadata["source_id"]
            for ds in self._datasets.values()
        ]

        if len(source_ids) != len(set(source_ids)):
            raise ValueError("Duplicate source IDs")

    @classmethod
    def from_yaml(cls, path):
        with Path(path).open("r", encoding="utf-8") as file:
            return cls(yaml.safe_load(file))

    @property
    def dataset_names(self):
        return tuple(self._datasets)

    def get_dataset(self, name):
        return self._datasets[name]

    def all_datasets(self):
        return tuple(self._datasets.values())

    def __len__(self):
        return len(self._datasets)
