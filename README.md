# VIP Data Processor

The VIP Data Processor listens to an [AWS SQS][SQS] queue for messages
about new data uploads to an [AWS S3][S3] bucket. It downloads the
data and validates it. Results are stored and able to distribute or be
reported upon.

[SQS]: http://aws.amazon.com/sqs/
[S3]: http://aws.amazon.com/s3/

## Running

### lein

To run the processor against a local .zip file run the following:

```sh
$ lein run ~/path/to/upload.zip
```

Currently this will validate the data and produce the XML output but
without any reporting whatsoever. Once the reporting infrastructure is
in place, a report will be generated so you can see errors and
warnings generated during validations.

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
containing an `:input`, processing functions in `:pipeline`, and
validation failures in `:warnings`, `:errors`, `:critical`, and
`:fatal`.

### The Processing Pipeline

`run-pipeline` function in the `vip.data-processor.pipeline` namespace
takes a context and will run thread that context through each function
in its `:pipeline`. Those processing functions may alter the
`:pipeline` (for example, to alter how uploads are loaded into the
database based on file type, or to add extra processing functions only
if all validations are successful).

Any processing function may add a `:stop` key to the processing
context which will halt the processing pipeline, returning the current
context as the final result.

Uncaught exceptions in a processing function will result in a `:stop`
key on the processing context and the exception being added as the
`:exception`.

### Processing Functions

#### Transforms

A processing function may alter the `:input` key for further
processing functions (e.g., `validation.transforms/download-from-s3`,
which takes the `:input` of an incoming context describing the
location of a file on S3, downloads that file, and associates that
file object to the `:input` key of the outgoing context). Such
functions can be called *transforms*.

#### Branches

A processing function may alter the `:pipeline` key, as well, allowing
for branching within a processing pipeline (e.g., XML and CSV files
will need to be transformed differently into a database
representation). Such functions can be called *branches*.

#### Validations

*Validation* functions check _something_ about the context, reporting
any failures in the `:warnings`, `:errors`, `:critical` or `:fatal`
keys of the processing context depending on the severity of the
failure.

The structure of those failure maps is the following: At the top level
is the severity. Next is the scope of the failure (e.g., the filename
or the XML node where the problem lies). Then the kind of problem
(e.g., `:duplicate-rows` or `:missing-headers`). And finally, a
message or sequence of bad data some further error message UI may use
to provide feedback to the use.

For example:

```clj
{:critical
  {"contest.txt"
    {:missing-headers ["election_id", "type"]
     :reference-error {"ballot_id" [{:line 4 "ballot_id" 108}
                                    {:line 6 "ballot_id" 109}]
                       "election_id" [{:line 2 "election_id" 210}]}}}}
```

Validation function are simply functions that accept a processing
context and return one, optionally adding failure report as
above. Typically, they'll look something like the following:

```clj
(defn validate-source-cromulence [ctx]
  (let [source-table (get-in ctx [:tables :sources])]
    (if (cromulent? source-table)
      ctx  ;; everything is okay, just return the context as-is
      (assoc-in ctx [:errors :source :cromulence]  ;; otherwise add error
                "Source is not cromulent"))))
```

To add a validation to all processing, it should be added to the
`pipeline` list in the `vip.data-processor` namespace. If it's only
required for XML or CSV processing, it should be added to the
`xml-validations` or `csv-validations` lists respectively in the
`vip.data-processor.validation.transforms` namespace.
