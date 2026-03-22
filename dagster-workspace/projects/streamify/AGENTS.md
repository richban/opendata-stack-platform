# Team Ops - Agent Notes

This file contains patterns and learnings for working with this codebase.

## Testing Dagster Assets with ConfigurableResources

When testing Dagster assets that use ConfigurableResource subclasses (like SparkConnectResource):

1. **Use @patch.object() to mock resource methods:**
   ```python
   @patch.object(SparkConnectResource, "get_session")
   def test_asset(self, mock_get_session):
       mock_session = MagicMock()
       mock_get_session.return_value = mock_session
       # ... rest of test
   ```

2. **Don't use MagicMock with spec:** Dagster validates resource types at bind time, and MagicMock with spec doesn't pass isinstance checks. Always use real resource objects with mocked methods.

3. **Use real resource objects in build_op_context():**
   ```python
   spark_resource = SparkConnectResource(...)
   context = build_op_context(resources={"spark": spark_resource})
   result = my_asset(context)  # Only pass context, not resources
   ```

## Asset Dependencies

Specify upstream dependencies in the decorator:
```python
@dg.asset(
    deps=[AssetKey("upstream_asset")],
)
def my_asset(...):
    ...
```

## Metadata Best Practices

- Use `MetadataValue.json()` for structured data
- Use `MetadataValue.text()` for simple strings
- Use `MetadataValue.url()` for links
- Access metadata values in tests via `.text`, `.value`, or `.data` attributes

## Type Hints for Flexible Dicts

When storing mixed types in dictionaries (e.g., asset metadata):
```python
# Use Any for value type to avoid type narrowing issues
results: dict[str, dict[str, Any]] = {}
```

## Scheduling Patterns

Create schedules using ScheduleDefinition with AssetSelection:
```python
my_schedule = dg.ScheduleDefinition(
    name="my_schedule",
    target=dg.AssetSelection.assets("my_asset"),
    cron_schedule="0 2 * * *",
)
```

Register in Definitions:
```python
defs = dg.Definitions(
    schedules=[my_schedule],
    ...
)
```

## Asset Checks

Use `@dg.asset_check` decorator to define data quality checks:
```python
@dg.asset_check(asset="silver_listen_events")
def silver_listen_events_not_empty(
    context: dg.AssetCheckExecutionContext,
    spark: SparkConnectResource,
    streaming_config: StreamingJobConfig,
) -> dg.AssetCheckResult:
    # Check logic here
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": dg.MetadataValue.int(row_count)},
    )
```

Register asset checks in Definitions:
```python
defs = dg.Definitions(
    asset_checks=dg.load_asset_checks_from_modules([dq_checks]),
    ...
)
```

## Testing Asset Checks

Asset checks return `AssetChecksDefinition` objects and should be tested by:
1. Verifying the check function is properly decorated
2. Testing the underlying logic/integrations separately
3. Using module-level imports to avoid PLC0415 violations

Example:
```python
from streamify.defs.dq_checks import silver_listen_events_not_empty

def test_dq_checks_are_asset_checks():
    assert isinstance(silver_listen_events_not_empty, dg.AssetChecksDefinition)
```

## SQLite Data Store Pattern

For simple append-only data storage (like DQ results), use SQLite with stdlib:
```python
import sqlite3

with sqlite3.connect(db_path) as conn:
    conn.execute("INSERT INTO table (...) VALUES (...)")
    conn.commit()
```

Store SQLite databases in a subdirectory (e.g., `dq_results/`) and add to `.gitignore`.
