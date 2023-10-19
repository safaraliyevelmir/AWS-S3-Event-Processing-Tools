import logging
from pathlib import Path
from urllib.parse import unquote

import boto3

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] - %(message)s", force=True)

logger = logging.getLogger("listener")
sqs = boto3.resource("sqs")
glue_client = boto3.client("glue")

def is_athena_table(path: str) -> bool:
    while path != ".":
        result = glue_client.search_tables(
            SearchText=path,
            Filters=[{'Key':'DatabaseName', 'Value':'ethereum'}],
        )
        if len(result["TableList"]) == 1:
            return True
        elif len(result["TableList"]) > 1:
            return False
        path = extract_parent(path)

    return False


def is_optimized_file(file_name: str):
    if "po" in file_name:
        return True
    return False

def extract_parent(raw_path: str) -> str:
    path = Path(raw_path)
    return unquote(str(path.parent))


def extract_file_name(raw_path: str) -> str:
    return Path(raw_path).name


def send_message_to_queue(item: str) -> None:
    queue_url = "" # TODO: add queue url
    queue = sqs.Queue(queue_url)

    # Send message to the queue with deduplication ID using the object_key
    parent_path = extract_parent(item)
    file_name = extract_file_name(item)

    if is_optimized_file(file_name):
        logger.info("This file is optimized %s", file_name)
        return

    if not is_athena_table(parent_path):
        logger.info("This file doesn't belong to Athena: %s", parent_path)
        return

    response = queue.send_message(
        MessageBody=parent_path, MessageGroupId=parent_path, MessageDeduplicationId=parent_path
    )

    # Check if the message was sent successfully
    assert response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200, "Failed to send item"
    logger.info("Sent Item: %s", item)


def lambda_handler(event, context) -> None:
    for record in event["Records"]:
        object_key = record["s3"]["object"]["key"]
        send_message_to_queue(object_key)

    logger.info("Finished Job")
