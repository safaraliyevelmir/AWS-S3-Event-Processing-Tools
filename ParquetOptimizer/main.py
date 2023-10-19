import logging
import uuid
from collections.abc import Iterable

import boto3
import fsspec
import numpy as np
import pandas as pd
import pyarrow
from pyarrow.lib import ArrowIOError

# Configure logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] - %(message)s", force=True)

logger = logging.getLogger(__name__)

# Initialize AWS clients and resources
sqs = boto3.client("sqs")
QUEUE_URL = "" # TODO: add queue url

s3 = boto3.resource("s3")
BUCKET_NAME = "" #TODO: add bucket name
s3_bucket = s3.Bucket(BUCKET_NAME)  # type: ignore


lambda_client = boto3.client("lambda")


def is_array(series: pd.Series) -> bool:
    """Checks if a given pandas Series contains any array-like objects."""
    return series.apply(lambda x: not (isinstance(x, list) or isinstance(x, np.ndarray))).all()


def construct_s3_uri(key: str) -> str:
    return f"s3://{BUCKET_NAME}/{key}"


def read_parquet(s3_file_path: str) -> pd.DataFrame | None:
    try:
        s3_uri = construct_s3_uri(s3_file_path)

        fs = fsspec.filesystem("s3")

        cache_options = {"simplecache": {"cache_storage": "cachedir"}}

        with fs.open(s3_uri, "rb", **cache_options) as f:
            df = pd.read_parquet(f, engine="pyarrow", dtype_backend="pyarrow")
    except ArrowIOError:
        logger.info("This file is unreadble %s", s3_uri)
        return None

    return df


def process_data(s3_objects: Iterable) -> tuple[pd.DataFrame, Iterable]:
    """
    Processes data by concatenating the Parquet files,
    dropping duplicates, and saving the result to a new file.
    """

    paruqet_files = {s3_object: read_parquet(s3_object.key) for s3_object in s3_objects}
    valid_files = {k: v for k, v in paruqet_files.items() if v is not None}

    df = pd.concat(list(valid_files.values()))

    subsets = df.apply(is_array)

    subsets_columns = subsets[subsets].index.tolist() or None

    logger.info("Check Array proccess finished %s", subsets_columns)

    df.drop_duplicates(subset=subsets_columns, inplace=True)

    logger.info("Data processing Finished")

    return df, list(valid_files.keys())


def calculate_num_chunks(df: pd.DataFrame) -> int:
    OPTIMAL_SIZE = 500  # MB

    total_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    logger.info("Total size of DataFrame %s (MB)", total_size_mb)

    total_size_mb /= 8
    logger.info("Expected total size of parquet files %s (MB)", total_size_mb)

    return max(round(total_size_mb / OPTIMAL_SIZE), 1)


def upload_data(df: pd.DataFrame, path: str) -> None:
    total_rows = len(df)

    if not total_rows:
        logger.info("DataFrame is empty")
        return

    num_chunks = calculate_num_chunks(df)
    rows_per_chunk = total_rows // num_chunks

    schema = pyarrow.Schema.from_pandas(df, preserve_index=False)
    logger.info("DataFrame schema - %s", str(schema))
    for i in range(0, total_rows, rows_per_chunk):
        chunk = df.iloc[i : i + rows_per_chunk]
        file_name = construct_s3_uri(path + "/" + "po-" + uuid.uuid4().hex)

        chunk.to_parquet(
            path=file_name,
            engine="pyarrow",
            compression="gzip",
            schema=schema,
            storage_options={"anon": False},
            index=False,
        )
        logger.info("Uploaded file to S3: %s", file_name)

    logger.info("All data uploaded successfully!")


def delete_data(s3_objects: Iterable) -> None:
    """Deletes the specified files from the S3 bucket."""
    for s3_object in s3_objects:
        s3_object.delete()

    logger.info("Deleting file: %s", s3_objects)


def process_message(message: dict) -> None:
    message_body = message["Body"]
    receipt_handle = message["ReceiptHandle"]

    logger.info("Received Message: %s", message_body)

    # Get the list of parquet files associated with the message from S3
    s3_objects = list(s3_bucket.objects.filter(Prefix=message_body).all())

    if not s3_objects:
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)

    logger.info("S3 objects are: %s", s3_objects)

    # Process the downloaded data and get the resulting file path
    data, s3_objects = process_data(s3_objects)

    print(s3_objects)

    # Upload the processed data file back to S3
    upload_data(data, message_body)

    # Delete the original S3 files
    delete_data(s3_objects)

    # Delete the processed message from the SQS queue
    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)


def lambda_handler(event, context):
    while True:
        try:
            # Receive messages from the SQS queue
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                VisibilityTimeout=900,
                WaitTimeSeconds=0,
            )

            if not response.get("Messages", False):
                logger.info("Response %s", response)
                break

            # Process each received message
            for message in response.get("Messages", []):
                process_message(message)

            logging.info("SQS messages processed successfully!")

        except Exception as e:
            # Log any errors that occurred during processing
            logger.error("An error occurred: %s", str(e))
