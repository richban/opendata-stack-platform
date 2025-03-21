from io import BytesIO
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from dagster import MaterializeResult, MetadataValue

from opendata_stack_platform.assets.taxi import taxi_zone_lookup_raw


@pytest.fixture()
def sample_csv_content():
    """Create a sample CSV content that matches
    the actual schema of the taxi zone file.
    """
    return b"""OBJECTID,Shape_Leng,the_geom,Shape_Area,zone,LocationID,borough
1,0.116357,"MULTIPOLYGON (((-74.1844529999 40.6974950001, -74.1849389999 40.6934140001)))",0.000782,"Newark Airport",1,"EWR"
2,0.287869,"MULTIPOLYGON (((-73.8525439999 40.7556599999, -73.8558609999 40.7513440001)))",0.005067,"Jamaica Bay",2,"Queens"
"""  # noqa: E501


def test_taxi_zone_lookup_raw(sample_csv_content):
    """Test the taxi_zone_lookup_raw asset function
    focusing on the outcomes rather than implementation details.
    """
    # Create a simple mock S3 resource with what we need
    mock_s3 = MagicMock()
    # Skip the implementation checking and just make sure we can access what we need
    mock_s3.get_client.return_value.put_object = MagicMock()

    # Mock the HTTP request
    with patch("requests.get") as mock_get:
        # Set up the mock HTTP response
        mock_response = MagicMock()
        mock_response.content = sample_csv_content
        mock_get.return_value = mock_response

        # Call the asset function
        result = taxi_zone_lookup_raw(mock_s3)

        # Verify our expectations

        # 1. The result should be a MaterializeResult
        assert isinstance(result, MaterializeResult)

        # 2. The result should contain the correct metadata
        assert "Number of records" in result.metadata
        assert result.metadata["Number of records"] == MetadataValue.int(2)

        # 3. Verify the request was made to the correct URL
        mock_get.assert_called_once_with(
            "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
        )


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
