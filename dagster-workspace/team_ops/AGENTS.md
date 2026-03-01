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
