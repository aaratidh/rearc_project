import os
import json
import boto3
import pandas as pd
from io import BytesIO

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET"]
POP_PREFIX = os.environ.get("POP_PREFIX", "raw/datausa/population/")
BLS_KEY = os.environ.get(
    "BLS_KEY",
    "bls-folder/pr/pr.data.0.Current"
)


def handler(event, context):
    print("Analytics Lambda triggered")

    # Load datasets
    population_df = load_latest_population()
    bls_df = load_bls_data()

    # -------------------------------
    # Part 3.1: Mean & Std Dev (2013–2018)
    # -------------------------------
    pop_stats = (
        population_df[
            (population_df["year"] >= 2013) &
            (population_df["year"] <= 2018)
        ]["population"]
        .agg(["mean", "std"])
    )

    print("Population Mean & StdDev:", pop_stats.to_dict())

    # -------------------------------
    # Part 3.2: Best year per series
    # -------------------------------
    yearly_sum = (
        bls_df
        .groupby(["series_id", "year"])["value"]
        .sum()
        .reset_index()
    )

    best_year = (
        yearly_sum
        .sort_values("value", ascending=False)
        .groupby("series_id")
        .first()
        .reset_index()
    )

    print("Best year per series (sample):")
    print(best_year.head(5).to_dict(orient="records"))

    # -------------------------------
    # Part 3.3: Join specific series + population
    # -------------------------------
    joined = (
        bls_df[
            (bls_df["series_id"] == "PRS30006032") &
            (bls_df["period"] == "Q01")
        ]
        .merge(population_df, on="year", how="inner")
        .sort_values("year")
    )

    print("Joined series + population:")
    print(joined.to_dict(orient="records"))

    return {"statusCode": 200}
def load_latest_population():
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=POP_PREFIX)
    latest = sorted(
        resp["Contents"],
        key=lambda x: x["LastModified"]
    )[-1]

    obj = s3.get_object(Bucket=BUCKET, Key=latest["Key"])
    data = json.load(obj["Body"])

    df = pd.DataFrame(data["data"])
    return df.rename(
        columns={
            "Year": "year",
            "Population": "population"
        }
    )[["year", "population"]]


def load_bls_data():
    obj = s3.get_object(Bucket=BUCKET, Key=BLS_KEY)

    df = pd.read_csv(
        obj["Body"],
        sep="\t",
        usecols=[
            "series_id        ",
            "year",
            "period",
            "       value"
        ]
    )

    return df.rename(
        columns={
            "series_id        ": "series_id",
            "       value": "value"
        }
    )
