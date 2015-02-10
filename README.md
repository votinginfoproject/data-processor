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
need to set them when you run the container. The following variables
need values:

* `VIP_DP_AWS_ACCESS_KEY`
* `VIP_DP_AWS_SECRET_KEY`
* `VIP_DP_S3_BUCKET`
* `VIP_DP_SQS_REGION`
* `VIP_DP_SQS_QUEUE`
* `VIP_DP_SQS_FAIL_QUEUE`
* `VIP_DP_RABBITMQ_EXCHANGE`
* `POSTGRES_USER`
* `POSTGRES_PASSWORD`

Running all dependent Docker containers is managed by
[docker-compose][docker-compose]. After installing that, bringing it
all up should be as simple as:

```sh
$ docker-compose build
$ docker-compose up
```

[docker-compose]: https://github.com/docker/fig

## Developing

The processing pipeline is defined as a sequence of functions that
take and return a processing context. The processing context is a map
containing an `:input`, further processing functions as `:pipeline`,
and maps containing `:warnings` and `:errors`.

Any processing function may add a `:stop` key to the processing
context which will halt the processing pipeline, returning the current
context as the final result.

Setting an `:exception` key will cause the processing pipeline to
throw its value (thereby placing a message on the failed processing
queue).

A processing function may alter the `:input` key for further
processing functions (e.g., `validation.transforms/download-from-s3`,
which takes the `:input` of an incoming context describing the
location of a file on S3, downloads that file, and associates that
file object to the `:input` key of the outgoing context). Such
functions can be called *transforms*.

A processing function may alter the `:pipeline` key, as well, allowing
for branching within a processing pipeline (e.g., XML and CSV files
will need to be transformed differently into a database
representation). Such functions can be called *branches*.

Processing functions that do not alter the `:input` or `:pipeline`
keys can be called *validations*.

When parallel processing steps of the pipeline are implemented, only
*validations* will be able to participate.

The `:warnings` and `:errors` keys are meant for messages a client
would find useful but which don't necessarily need to stop processing
from continuing. For example, illegal values in a data set would
prohibit the data set from being acceptable, but further processing
may be able to be accomplished to find more issues a client could
correct without having their data rejected each time at the first
problem.

Uncaught exceptions in a processing function will result in a `:stop`
key on the processing context and the exception being added as the
`:exception`.
