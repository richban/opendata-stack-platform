"""Dagster assets for the batch (slow) layer.

Batch transformation layer: bronze (raw) -> silver (typed), with routing.

Reads the lossless, schema-agnostic ``bronze_<topic>`` table, classifies every
record against the deployed consumer contract, and fans out:

* READY  -> decoded and merged into ``silver_<topic>`` (idempotent on ``event_id``)
* DRIFT  -> ``quarantine_schema_drift`` (retryable: replay after an upgrade)

Corrupt/unparseable payloads are dead-lettered by the *stream* at decode time,
so this batch layer does not write the DLQ.

Because bronze is the source of truth, this job can be re-run at any time over
any ``_ingest_date`` range; the Iceberg MERGEs make each re-run converge rather
than duplicate. That is also the reprocessing path: after the consumer contract
is widened, re-running this job moves previously parked records into silver.
"""

# TODO: