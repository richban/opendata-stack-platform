from dagster import (
    RunConfig,
    RunRequest,
    SkipReason,
    sensor,
    SensorEvaluationContext,
    SensorDefinition,
)
from dagster_aws.s3.sensor import get_s3_keys
from dagster_aws.s3 import S3Resource

AWS_S3_BUCKET = "datalake"
AWS_S3_OBJECT_PREFIX = "raw"


def make_s3_sensor(job) -> SensorDefinition:
    """Returns a sensor that launches the given job."""

    @sensor(name=f"{job.name}", job=job)
    def s3_sensor(context: SensorEvaluationContext, s3: S3Resource):
        latest_key = context.cursor or None
        unprocessed_object_keys = get_s3_keys(
            bucket=AWS_S3_BUCKET,
            prefix=AWS_S3_OBJECT_PREFIX,
            since_key=latest_key,
            s3_session=s3.get_client(),
        )

        for key in unprocessed_object_keys:
            context.log.info(f"processing key: {key}")
            yield RunRequest(
                run_key=key,
                run_config=RunConfig(
                    ops={
                        "input_split_portfolio_to_rows": {
                            "inputs": {"df": {"key": key}}
                        }
                    }
                ),
            )

        if not unprocessed_object_keys:
            return SkipReason("No new s3 files found for bucket source-bucket.")

        last_key = unprocessed_object_keys[-1]
        context.update_cursor(last_key)

    return s3_sensor
