import pendulum
from datetime import timedelta
from snowflake.snowpark.types import StructType, StructField, StringType

def main(session):
    bucket_name = "my-bucket"
    target_day = "2025-07-24"
    timezone = "Asia/Kolkata"
    interval_minutes = 15
    table_name = "your_existing_table"

    base = pendulum.parse(f"{target_day}T00:00:00", tz=timezone)

    records = []
    intervals_per_day = int(24 * 60 / interval_minutes)

    for i in range(intervals_per_day):
        start = base.add(minutes=i * interval_minutes)
        end = base.add(minutes=(i + 1) * interval_minutes)
        start_iso = start.to_iso8601_string()
        end_iso = end.to_iso8601_string()
        hh_mm = start.format("HH-mm")
        c3 = f"s3://{bucket_name}/{target_day}/{hh_mm}"
        records.append((start_iso, end_iso, c3))

    schema = StructType([
        StructField("start_time", StringType()),
        StructField("end_time", StringType()),
        StructField("c3", StringType())
    ])

    df = session.create_dataframe(records, schema)
    df.write.mode("append").save_as_table(table_name)

    return df
