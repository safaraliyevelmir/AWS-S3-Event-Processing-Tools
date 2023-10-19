# AWS Lambda Functions for S3 Event Processing

This repository contains two AWS Lambda functions for processing events related to an Amazon S3 bucket. The functions are designed for specific use cases and can be configured to automate tasks involving S3 events. Below are details on each Lambda function:

## Table of Contents

1. [ParquetOptimizer Lambda Function](#1-parquetoptimizer-lambda-function)
   - [Prerequisites](#prerequisites)
   - [Configuration](#configuration)
   - [Usage](#usage)

2. [S3 Event Listener Lambda Function](#2-s3-event-listener-lambda-function)
   - [Prerequisites](#prerequisites-1)
   - [Configuration](#configuration-1)
   - [Usage](#usage-1)

## 1. ParquetOptimizer Lambda Function

The ParquetOptimizer is an AWS Lambda function that processes Parquet files stored in an Amazon S3 bucket. The function is triggered by messages sent to an Amazon SQS queue. Each message contains the prefix of the S3 files to be processed. The function downloads the files, concatenates them, drops duplicates, and saves the result to a new file. The new file is then uploaded back to S3, and the original files are deleted.

### Prerequisites

Before using the ParquetOptimizer Lambda function, you need to set up the following:

1. **Amazon SQS Queue**: Create an Amazon SQS queue where you will send messages containing the prefixes of the S3 files to be processed.

2. **Amazon S3 Bucket**: Create an Amazon S3 bucket where the Parquet files are stored and where the optimized files will be saved.

3. **Lambda Function**: Create an AWS Lambda function and configure it to use the SQS queue as a trigger.

4. **Source Code**: Upload the `ParquetOptimizer.zip` file as the source code for your Lambda function.

5. **Environment Variables**: Configure the Lambda function with the following environment variables:
   - `BUCKET_NAME`: The name of the S3 bucket.
   - `QUEUE_URL`: The URL of the SQS queue.

6. **IAM Role and Policy**: Create an IAM role that allows the Lambda function to access the SQS queue and the S3 bucket. Create a policy that grants access to these resources and attach the policy to the IAM role.

7. **Upload Code and Data**: Upload the `ParquetOptimizer.zip` file to your S3 bucket and upload the Parquet files you want to process to the same S3 bucket.

8. **Send SQS Messages**: Send a message to the SQS queue with the prefix of the S3 files to be processed.

### Configuration

In the Lambda function code, you may need to adjust the following settings:

- `QUEUE_URL`: Update the `QUEUE_URL` variable to match the URL of your SQS queue.

- `BUCKET_NAME`: Update the `BUCKET_NAME` variable to match the name of your S3 bucket.

- `OPTIMAL_SIZE`: This constant represents the target size of the Parquet files. You can adjust this value to control the size of the output files.

### Usage

Once you have set up the ParquetOptimizer Lambda function and configured the necessary resources, you can start using it as follows:

1. Send a message to the SQS queue with the prefix of the S3 files you want to process.

2. The Lambda function will process the files, optimize them, and upload the resulting file back to the S3 bucket.

3. The original Parquet files will be deleted from the S3 bucket.

4. The Lambda function will continue to process messages from the SQS queue as long as there are messages to process.

## 2. S3 Event Listener Lambda Function

The S3 Event Listener is an AWS Lambda function that listens for new files in the Ethereum S3 bucket and sends a message to the SQS queue for each new file. The message contains the path to the file in the S3 bucket. The SQS queue is configured as a FIFO queue, which means that the messages are processed in the order they are received. This is important because the Athena queries are dependent on the order of the files in the S3 bucket.

### Prerequisites

Before using the S3 Event Listener Lambda function, you need to set up the following:

1. **Amazon SQS Queue**: Create an Amazon SQS queue where you will receive messages for new files in the S3 bucket.

2. **Amazon S3 Bucket**: Create an Amazon S3 bucket where new files are expected to be added and monitored.

3. **Lambda Function**: Create an AWS Lambda function and configure it to use the SQS queue as a trigger.

4. **Source Code**: Upload the `S3EventListener.zip` file as the source code for your Lambda function.

5. **Environment Variables**: Configure the Lambda function with the following environment variables:
   - `BUCKET_NAME`: The name of the S3 bucket.
   - `QUEUE_URL`: The URL of the SQS queue.

6. **IAM Role and Policy**: Create an IAM role that allows the Lambda function to access the SQS queue and the S3 bucket. Create a policy that grants access to these resources and attach the policy to the IAM role.

7. **Upload Code and Data**: Upload the `S3EventListener.zip` file to your S3 bucket and ensure that it contains the Lambda function code.

8. **Monitoring Prefix**: Send a message to the SQS queue with the prefix of the S3 files that the Lambda function should monitor.

### Configuration

In the Lambda function code, you may need to adjust the following settings:

- `QUEUE_URL`: Update the `QUEUE_URL` variable to match the URL of your SQS queue.

- `BUCKET_NAME`: Update the `BUCKET_NAME` variable to match the name of your S3 bucket.

### Usage

Once you have set up the S3 Event Listener Lambda function and configured the necessary resources, you can start using it as follows:

1. New files added to the S3 bucket will trigger the Lambda function, which will send messages to the SQS queue.

2. The Lambda function checks if the file is optimized or not. If it is optimized, it is not sent to the queue. If it is not optimized, it checks if it belongs to Athena. If it doesn't belong to Athena, it is not sent to the queue.

3. The SQS queue will receive and process messages for further actions or workflows.

