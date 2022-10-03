#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging

import pandas as pd

import wandb

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact.
    local_path = wandb.use_artifact("sample.csv:latest").file()
    df = pd.read_csv(local_path)

    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df["price"].between(min_price, max_price)
    df = df[idx].copy()
    idx = df["longitude"].between(-74.25, -73.50) & df["latitude"].between(40.5, 41.2)
    df = df[idx].copy()
    # Convert last_review to datetime
    df["last_review"] = pd.to_datetime(df["last_review"])

    df.to_csv("clean_sample.csv", index=False)

    logger.info(f"Uploading {args.output_artifact} to Weights & Biases")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    # We need to call this .wait() method before we can use the
    # version below. This will wait until the artifact is loaded into W&B and a
    # version is assigned
    artifact.wait()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Artifact location from previous step",
        required=True,
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Where to store cleaned data artifact ",
        required=True,
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type of output artifact",
        required=True,
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description for output artifact",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=int,
        help="Minimum allowed price",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        type=int,
        help="Maximum allowed price",
        required=True,
    )

    args = parser.parse_args()

    go(args)
