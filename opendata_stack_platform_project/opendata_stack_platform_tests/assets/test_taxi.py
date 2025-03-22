from io import BytesIO
from unittest.mock import MagicMock, patch

import boto3
import polars as pl
import pytest

from dagster import (
    MaterializeResult,
    MetadataValue,
    build_op_context,
)
from dagster_aws.s3 import S3Resource
from moto import mock_aws

from opendata_stack_platform.assets import constants
from opendata_stack_platform.assets.taxi import taxi_zone_lookup_raw, yellow_taxi_trip_raw
from opendata_stack_platform.utils.download_and_upload_file import (
    download_and_upload_file,
)


@pytest.fixture()
def _aws_credentials(monkeypatch):
    # Override credentials for moto
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # Remove endpoint overrides to ensure moto is used
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("ENDPOINT_URL", raising=False)

    # Optionally override other vars if needed
    monkeypatch.setenv("AWS_REGION", "us-east-1")


@pytest.fixture()
def s3_resource(_aws_credentials) -> boto3.client:
    """
    Return a mocked S3 resource for Dagster
    """
    with mock_aws():
        session = boto3.Session()
        s3_client = session.client("s3")
        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        s3_client.create_bucket(Bucket=constants.BUCKET)
        yield s3_client


@pytest.fixture()
def sample_csv_content() -> bytes:
    """Create a sample CSV content that matches
    the actual schema of the taxi zone file.
    """
    return b"""OBJECTID,Shape_Leng,the_geom,Shape_Area,zone,LocationID,borough
1,0.116357,"MULTIPOLYGON (((-74.1844529999 40.6974950001, -74.1849389999 40.6934140001)))",0.000782,"Newark Airport",1,"EWR"
2,0.287869,"MULTIPOLYGON (((-73.8525439999 40.7556599999, -73.8558609999 40.7513440001)))",0.005067,"Jamaica Bay",2,"Queens"
"""  # noqa: E501


def test_taxi_zone_lookup_raw(s3_resource: boto3.client, sample_csv_content):
    """Test the taxi_zone_lookup_raw asset function
    focusing on the outcomes rather than implementation details.
    """
    # Create a patch for S3Resource.get_client
    with patch.object(S3Resource, "get_client", return_value=s3_resource) as _:
        # Build the context with the actual S3Resource
        context = build_op_context(resources={"s3": S3Resource()})

        # Mock the HTTP request
        with patch("requests.get") as mock_get:
            # Set up the mock HTTP response
            mock_response = MagicMock()
            mock_response.content = sample_csv_content
            mock_get.return_value = mock_response

            # Call the asset function
            result = taxi_zone_lookup_raw(context)

            # Verify our expectations

            # 1. The result should be a MaterializeResult
            assert isinstance(result, MaterializeResult)

            # 2. The result should contain the correct metadata
            assert "Number of records" in result.metadata
            assert result.metadata["Number of records"] == MetadataValue.int(2)

            # 3. Verify the request was made to the correct URL
            mock_get.assert_called_once_with(
                "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
            )

            # 5. Verify that the file was uploaded to the correct location in S3
            response = s3_resource.list_objects_v2(
                Bucket=constants.BUCKET, Prefix=constants.TAXI_ZONES_FILE_PATH
            )

            # Verify the object exists
            assert "Contents" in response, (
                f"File {constants.TAXI_ZONES_FILE_PATH} not found in bucket {constants.BUCKET}"  # noqa: E501
            )
            assert (
                len(response["Contents"]) == 1
            ), f"Expected exactly one file with prefix {constants.TAXI_ZONES_FILE_PATH}"
            assert response["Contents"][0]["Key"] == constants.TAXI_ZONES_FILE_PATH


def test_yellow_taxi_trip_raw():
    """Test the yellow_taxi_trip_raw asset function with partitioning."""

    # Create a Dagster context with partition key and resources
    context = build_op_context(partition_key="2023-01-01", resources={"s3": S3Resource()})

    # Patch the download_and_upload_file function
    with patch(
        "opendata_stack_platform.assets.taxi.download_and_upload_file"
    ) as mock_download:
        # Call the asset function
        yellow_taxi_trip_raw(context)

        # Verify download_and_upload_file was called with the correct arguments
        mock_download.assert_called_once_with(
            context,
            S3Resource(),
            dataset_type="yellow",
        )


def test_download_and_upload_file(s3_resource):
    """Test the download_and_upload_file function using moto to mock AWS services."""

    # Create the bucket
    s3_resource.create_bucket(Bucket=constants.BUCKET)

    # Mock the S3Resource to use our mocked client
    mocked_s3_resource = MagicMock(spec=S3Resource)
    mocked_s3_resource.get_client.return_value = s3_resource

    # Create a Dagster context with partition key and resources
    context = build_op_context(
        partition_key="2023-01-01", resources={"s3": mocked_s3_resource}
    )

    # Patch the requests.get function
    with patch("requests.get") as mock_get:
        # Set up the mock response
        mock_response = MagicMock()
        sample_data = b"trip_id,pickup_datetime,passenger_count\n1,2023-01-01 12:00:00,2\n2,2023-01-02 13:00:00,3"  # noqa: E501
        mock_response.content = sample_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the function with our mocked resource
        download_and_upload_file(context, mocked_s3_resource, dataset_type="yellow")

        # Verify the file was uploaded
        expected_key = f"raw/yellow/yellow_tripdata_{context.partition_key}.parquet"
        response = s3_resource.list_objects_v2(
            Bucket=constants.BUCKET, Prefix="raw/yellow"
        )

        objects = response.get("Contents", [])
        assert len(objects) >= 1

        matching_objects = [obj for obj in objects if obj["Key"] == expected_key]
        assert len(matching_objects) == 1


def test_taxi_zone_data_has_required_columns(sample_csv_content):
    """Test that the CSV data contains all required columns."""
    # Parse the CSV content
    df = pl.read_csv(BytesIO(sample_csv_content))

    # Check that all expected columns exist
    expected_columns = [
        "OBJECTID",
        "Shape_Leng",
        "the_geom",
        "Shape_Area",
        "zone",
        "LocationID",
        "borough",
    ]

    for col in expected_columns:
        assert col in df.columns, f"Missing required column: {col}"

    # Check the types of some key columns
    assert df.schema["LocationID"] == pl.Int64
    assert df.schema["zone"] == pl.Utf8
    assert df.schema["borough"] == pl.Utf8
