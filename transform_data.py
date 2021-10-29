import logging
from datetime import datetime

import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count
import pyspark.sql.functions as F

logging.basicConfig(
    filename=f"logs/transform_data",
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("transform_data")
logger.info(f"Starting another run at {datetime.now()}.")


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.option(
    "--ad_stream_path",
    default="./data/ads_stream.csv",
    help="Path of the ad stream csv file.",
)
@click.option(
    "--store_stream_path",
    default="./data/store_stream.csv",
    help="Path of the store stream csv file.",
)
@click.option(
    "--path_to_store_data", default="./data/", help="Path to store processed data."
)
def main(ad_stream_path, store_stream_path, path_to_store_data):
    spark = SparkSession.builder.appName("KPI Dashboard").getOrCreate()

    ad_stream_df = load_data(spark, ad_stream_path)
    store_stream_df = load_data(spark, store_stream_path)

    # Ignoring NaN values within ad_id column because event type of log will
    # always have NaN values for ad_id.
    ad_stream_cleaned = clean_data(ad_stream_df, ignore_nans_in=["ad_id"])

    # Ignoring NaN values within app_id column because event type of log will
    # always have NaN values for app_id.
    store_stream_cleaned = clean_data(store_stream_df, ignore_nans_in=["app_id"])

    # We're defining two types of events -- engaging events and unengaging events.
    # engaging events require user's attention and effort. These events are used to
    # define active users. Example -- click on an ad, if a user clicks on an ad, then
    # we can be sure that the user has at least seen the ad and has taken an action.
    # unengaging events are often system generated or don't require any action on
    # user's behalf. Example -- displaying of an ad on your mobile app, just display
    # of an ad doesn't guarantee that the user has actually seen the ad.

    # There are 5 types of ad events:
    # engaging events -- click, close
    # unengaging events -- logs, display, load
    engaging_ad_events = ["click", "close"]

    # There are 5 types of app store events:
    # engaging events -- app_view, open, download
    # unengaging events -- logs
    engaging_store_events = ["app_view", "open", "download"]

    ad_stream_processed = process_data(ad_stream_cleaned, engaging_ad_events)
    # ad_stream_processed.show()

    store_stream_processed = process_data(store_stream_cleaned, engaging_store_events)
    # print(store_stream_processed.show())

    dataframes = create_transformed_dataframes(
        ad_stream_processed, store_stream_processed
    )
    for df, name in dataframes:
        filepath = f"{path_to_store_data}{name}"
        df.coalesce(1).write.options(header="True").csv(filepath)

    print(
        "Completed creating dataframes. You can now upload them to your Dashboarding tool."
    )


def load_data(spark, filepath):
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    return df


def clean_data(df, ignore_nans_in=list):
    df = handle_nan(df, ignore_nans_in)
    df = drop_duplicates(df)
    df = convert_timestamp(df, "server_time")
    df = add_additional_date_columns(df, "server_time")
    return df


def process_data(df, engaging_events):
    df = add_user_active_status(df, engaging_events)
    return df


def add_user_active_status(df, engaging_events):
    df = df.withColumn(
        "active", F.when(df.event_type.isin(engaging_events), 1).otherwise(0)
    )
    return df


def create_transformed_dataframes(ad_df, store_df):
    mau = calculate_monthly_active_users(ad_df, store_df)
    dau = calculate_daily_active_users(ad_df, store_df)
    ad_performance = calculate_advertisement_performance(ad_df)
    app_performance = calculate_apps_on_store_performance(store_df)
    return [
        (mau, "mau"),
        (dau, "dau"),
        (ad_performance, "ad_performance"),
        (app_performance, "app_performance"),
    ]


def calculate_monthly_active_users(ad_df, store_df):
    subset = ["year-month", "device_id", "country", "active"]
    df = ad_df.select(subset).union(store_df.select(subset))
    monthly_metric = df.groupBy(["device_id", "year-month", "country"]).max("active")
    monthly_metric = monthly_metric.sort(
        ["year-month", "device_id", "country"], ascending=[1, 1, 1]
    )
    return monthly_metric.withColumnRenamed("max(active)", "active")


def calculate_daily_active_users(ad_df, store_df):
    subset = ["date", "device_id", "country", "active"]
    df = ad_df.select(subset).union(store_df.select(subset))
    daily_metric = df.groupBy(["device_id", "date", "country"]).max("active")
    daily_metric = daily_metric.sort(
        ["date", "device_id", "country"], ascending=[1, 1, 1]
    )
    return daily_metric.withColumnRenamed("max(active)", "active")


def calculate_advertisement_performance(df):
    subset = ["device_id", "ad_id", "year-month", "event_type", "country"]

    # For a particular month, on how many unique devices a particular ad was displayed.
    display_df = calculate_unique_monthly_devices(
        for_column="ad_id", event_type="display", df=df.select(subset)
    )

    # For a particular month, on how many devices a particular ad was clicked.
    click_df = calculate_unique_monthly_devices(
        for_column="ad_id", event_type="click", df=df.select(subset)
    )

    # For a particular month, what was the CTR for each ad.
    result = left_join(display_df, click_df)
    result = result.withColumn("CTR", 100 * (F.col("num_click") / F.col("num_display")))

    result = result.na.fill(value=0)
    return result


def calculate_apps_on_store_performance(df):
    subset = ["device_id", "app_id", "year-month", "event_type", "country"]

    # For a particular month, on how many unique devices a particular app was viewed.
    app_viewed_df = calculate_unique_monthly_devices(
        for_column="app_id", event_type="app_view", df=df.select(subset)
    )

    # For a particular month, on how many devices a particular app was downloaded.
    downloaded_df = calculate_unique_monthly_devices(
        for_column="app_id", event_type="download", df=df.select(subset)
    )

    # For a particular month, what percentage of times an app downloaded for each view it had?
    result = left_join(app_viewed_df, downloaded_df)
    result = result.withColumn(
        "perc_downloads_per_app_view",
        100 * (F.col("num_download") / F.col("num_app_view")),
    )

    result = result.na.fill(value=0)
    return result


def handle_nan(df, ignore_nans_in):
    nan_df = check_for_nan(df)

    subset = []
    for col in nan_df.columns:
        if nan_df[col].values > 0 and col not in ignore_nans_in:
            subset.append(col)
            logger.warn(
                f"Found {nan_df[col].values} NaN unexpectedly in {col} "
                f"Will drop all rows with NaN value in this column."
            )
    num_rows = df.count()
    df = df.dropna(subset=subset)
    num_rows_after_drop = df.count()
    logger.info(
        f"{str(num_rows - num_rows_after_drop)} rows dropped because of NaN"
        f". {str(num_rows)} before dropping, {str(num_rows_after_drop)} "
        f"after dropping."
    )
    return df


def check_for_nan(df, subset=None):
    if subset:
        df = df.select(subset)
    for column in df.columns:
        df = df.withColumn(column, df["`{}`".format(column)].cast("string"))
    nan_df = df.select(
        [
            count(
                when(
                    col(c).contains("None")
                    | col(c).contains("NULL")
                    | col(c).contains("NaN")
                    | (col(c) == "")
                    | col(c).isNull()
                    | isnan(c),
                    c,
                )
            ).alias(c)
            for c in df.columns
        ]
    )
    return nan_df.toPandas()


def drop_duplicates(df):
    num_rows = df.count()
    distinct_df = df.distinct()
    after_drop = distinct_df.count()
    logger.info(
        f"{str(num_rows - after_drop)} rows dropped because of duplication. "
        f"Before: {num_rows}, after dropping duplicates: {after_drop}."
    )
    return distinct_df


def convert_timestamp(df, timestamp_column, replace_original=True):
    df_with_timestamp = df.withColumn(
        "timestamp", F.to_timestamp(timestamp_column, "M/d/yy H:mm")
    )

    # Making sure timestamp conversion took place accurately without creating any new NaN values
    nan_df = check_for_nan(df_with_timestamp, subset=["timestamp"])
    if nan_df["timestamp"].values[0] > 0:
        raise ValueError(
            f"Conversion of {timestamp_column} to type timestamp generated unexpected"
            f"NaN values."
        )

    if replace_original:
        df_with_timestamp = df_with_timestamp.drop(timestamp_column)
        df_with_timestamp = df_with_timestamp.withColumnRenamed(
            "timestamp", timestamp_column
        )
    return df_with_timestamp


def add_additional_date_columns(df, timestamp_column):
    temp_df = df.withColumn("date", F.to_date(timestamp_column))
    temp_df = temp_df.withColumn(
        "year-month", F.date_format(timestamp_column, "yyyy-MM")
    )
    return temp_df


def rename_columns(df, columns):
    if isinstance(columns, dict):
        return df.select(
            *[
                F.col(col_name).alias(columns.get(col_name, col_name))
                for col_name in df.columns
            ]
        )
    else:
        raise ValueError(
            "'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}"
        )


def calculate_unique_monthly_devices(for_column, event_type, df):
    df = df.filter(df.event_type == event_type)
    df = df.groupBy(["country", for_column, "year-month"]).agg(
        F.countDistinct("device_id")
    )
    df = df.withColumnRenamed("count(device_id)", f"num_{event_type}")
    return df


def left_join(left_df, right_df):
    left_columns, right_columns = set(left_df.columns), set(right_df.columns)
    common_columns = left_columns.intersection(right_columns)

    new_names = {col: f"{col}_right" for col in common_columns}
    right_df = rename_columns(right_df, new_names)

    cond = [left_df[f"{col}"] == right_df[f"{col}_right"] for col in common_columns]
    result = left_df.join(right_df, cond, "left")

    return result.drop(*new_names.values())


if __name__ == "__main__":
    main()
