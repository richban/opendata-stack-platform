from typing import Union, Dict, Any, Optional

import fsspec
import polars as pl
import pyarrow.dataset as ds
from dagster import InputContext, OutputContext
from upath import UPath

from dagster_polars.io_managers.base import BasePolarsUPathIOManager


class PolarsCSVIOManager(BasePolarsUPathIOManager):
    base_dir: Optional[str] = None
    extension: str = ".csv"
    storage_options: Dict[str, Any] = None

    assert BasePolarsUPathIOManager.__doc__ is not None
    __doc__ = (
        BasePolarsUPathIOManager.__doc__
        + """\nWorks with CSV files.
    All read/write arguments can be passed via corresponding metadata values."""
    )

    def write_df_to_path(self, context: OutputContext, df: pl.DataFrame, path: UPath):
        assert context.metadata is not None

        with path.open("wb") as file:
            df.write_csv(
                file,
                include_header=context.metadata.get("has_header", True),
                separator=context.metadata.get("separator", ","),
                quote_char=context.metadata.get("quote", '"'),
                batch_size=context.metadata.get("batch_size", 1024),
                datetime_format=context.metadata.get("datetime_format"),
                date_format=context.metadata.get("date_format"),
                time_format=context.metadata.get("time_format"),
                float_precision=context.metadata.get("float_precision"),
                null_value=context.metadata.get("null_value"),
            )

    def sink_df_to_path(
        self,
        context: OutputContext,
        df: pl.LazyFrame,
        path: "UPath",
    ):
        context_metadata = context.metadata or {}

        # Collect the LazyFrame to a DataFrame
        df_collected = df.collect()

        # Open the path and write the DataFrame as a CSV
        with path.open("wb") as file:
            df_collected.write_csv(
                file,
                has_header=context_metadata.get("has_header", True),
                separator=context_metadata.get("separator", ","),
                quote=context_metadata.get("quote", '"'),
                batch_size=context_metadata.get("batch_size", 1024),
                datetime_format=context_metadata.get("datetime_format"),
                date_format=context_metadata.get("date_format"),
                time_format=context_metadata.get("time_format"),
                float_precision=context_metadata.get("float_precision"),
                null_value=context_metadata.get("null_value"),
            )

    def scan_df_from_path(self, path: UPath, context: InputContext) -> pl.LazyFrame:
        assert context.metadata is not None

        fs: Union[fsspec.AbstractFileSystem, None] = None

        try:
            fs = path._accessor._fs
        except AttributeError:
            pass

        return pl.scan_pyarrow_dataset(
            ds.dataset(
                source=str(path),
                filesystem=fs,
                format=context.metadata.get("format", "csv"),
                partitioning=context.metadata.get("partitioning"),
                partition_base_dir=context.metadata.get("partition_base_dir"),
                exclude_invalid_files=context.metadata.get(
                    "exclude_invalid_files", True
                ),
                ignore_prefixes=context.metadata.get("ignore_prefixes", [".", "_"]),
            ),
            allow_pyarrow_filter=context.metadata.get("allow_pyarrow_filter", True),
        )

    def _get_path(self, context: Union[InputContext, OutputContext]) -> "UPath":
        """
        Determine the path based on the type of context:
        - Uses `config["key"]` only if the context is an `InputContext`, has no `asset_key`, and has a config with "key".
        - Otherwise, defaults to the parent class's path logic.
        """
        if (
            isinstance(context, InputContext)
            and not context.has_asset_key
            and context.config.get("key")
        ):
            # loading input based on provided input config `key`
            return UPath(self.base_dir, context.config["key"])

        return super()._get_path(context)
