# VIP Data Processor

## Docker

```sh
$ docker build -t vip-data-processor .
$ docker run -e AWS_ACCESS_KEY=**your aws key** \
             -e AWS_SECRET_KEY=**your aws secret key** \
             -e S3_BUCKET=**your s3 bucket** \
             -e SQS_REGION=**your sqs region** \
             -e SQS_QUEUE=**your sqs queue** \
             -e SQS_FAIL_QUEUE=**your sqs failure queue** \
             vip-data-processor
```
