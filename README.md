# VIP Data Processor

The VIP Data Processor listens to an [AWS SQS][SQS] queue for messages
about new data uploads to an [AWS S3][S3] bucket. It downloads the
data and validates it. Results are stored and able to distribute or be
reported upon.

[SQS]: http://aws.amazon.com/sqs/
[S3]: http://aws.amazon.com/s3/

## Running

### lein

Copy `dev-resources/config.edn.sample` to `dev-resources/config.edn`
and set the values inside as necessary. Then:

```sh
$ lein trampoline run
```

### Docker

Configuration in Docker is set with environment variables, so you'll
need to set them when you run the container.

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
