from datetime import date, datetime

from pyspark.sql import Row
from streamify.defs.resources import create_spark_session, get_streaming_config


def main():
    config = get_streaming_config()
    spark = create_spark_session(config)

    df = spark.createDataFrame(
        [
            Row(
                a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)
            ),
            Row(
                a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)
            ),
            Row(
                a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)
            ),
        ]
    )
    df.show()


if __name__ == "__main__":
    main()
