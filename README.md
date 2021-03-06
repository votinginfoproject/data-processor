# VIP Data Processor

The VIP Data Processor listens to an [AWS SQS][SQS] queue for messages
about new data uploads to an [AWS S3][S3] bucket. It downloads the
data and validates it. Results are stored and able to distribute or be
reported upon.

[SQS]: http://aws.amazon.com/sqs/
[S3]: http://aws.amazon.com/s3/

## Running

### Setup
1. Install postgres 11 (`brew install postgresql11`)
1. Install pgadmin 4 (handy GUI to administer postgres): `brew cask install pgadmin4`
1. Once both are installed, make sure postgres is running; If it is not, you can run `brew services start postgresql@11`
1. Bring up pgadmin4 via MacOS Applications
    1. You will need to create a Master Password
    1. You will then create a new Server that connects to localhost
1. Within _Login/Group Roles_ in pgadmin4, Create a role called `dataprocessor`; Use the below SQL for guidance
    ```sql
	CREATE ROLE dataprocessor LOGIN
	ENCRYPTED PASSWORD 'vip2016' SUPERUSER INHERIT CREATEDB CREATEROLE NOREPLICATION;
    ```
1. Within _Databases_ in pgadmin4, Create a database called `dataprocessor`. Use the below SQL for guidance
    ```sql
    CREATE DATABASE dataprocessor
    WITH OWNER = dataprocessor
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    CONNECTION LIMIT = -1;
    ```


### lein

To run the processor against a local .zip, you'll need to have followed the above Postgres
steps and have it running and a minimal conifguration file.

Create `dev-resources/config.edn` with a map that looks like the
following (using values for your Postgres server):

```clj
{:postgres {:database "dataprocessor"
            :host "localhost"
            :port 5432
            :user "dataprocessor"
            :password nil}}
```

Then use `lein run` to process a zip file:

```sh
$ lein run ~/path/to/upload.zip
```

This will validate the data, import it into Postgres, and generate an
XML feed.

The last line logged will be a Clojure map with information about the
run, including:

* its `import-id` which is used pervasively in the Postgres database
  as the `results_id` column in almost every table
* the location of the SQLite scratch database used during processing
* the location of the XML file generated

When diagnosing problems with processing, those can help you get
started.

### Docker

If you'd like to run data-processor locally exactly as it's run in
production, you will need Docker.

Configuration in Docker is set via the `.env` file in the root of the
project. Create that file, copy the following into it and provide
values for each environment variable.

```
DB_PORT_5432_TCP_ADDR=
DB_ENV_POSTGRES_USER=
DB_ENV_POSTGRES_PASSWORD=
DB_ENV_POSTGRES_DATABASE=
POSTGRES_USER=
POSTGRES_PASSWORD=
VIP_DP_AWS_ACCESS_KEY=
VIP_DP_AWS_SECRET_KEY=
VIP_DP_AWS_REGION=
VIP_DP_S3_UNPROCESSED_BUCKET=
VIP_DP_S3_PROCESSED_BUCKET=
VIP_DP_SQS_QUEUE=
VIP_DP_SQS_FAIL_QUEUE=
VIP_DP_SNS_SUCCESS_TOPIC=
VIP_DP_SNS_FAILURE_TOPIC=
VIP_DP_MAX_ZIPFILE_SIZE=(3221225472 = 3GB by default)
```

As you can see, you'll need to have set up an S3 bucket and two SQS
queues and two SNS topics.

The S3 bucket is where the processed results file ends up (if applicable).

The SQS queues are for initiating feed processing with a message structured like:
`{:filename "path/to/file.zip" :bucket "source-s3-bucket"}`. Sending this to
the queue configured as `VIP_DP_SQS_QUEUE` will kick off processing of the file located
in `source-s3-bucket/path/to/file.zip`. If, for some reason, processing fails, a copy of
the message is sent to the `VIP_DP_SQS_FAIL_QUEUE`, wrapped with some extra info about the attempt
and the cause of the failure.

The SNS topics are for broadcasting the results of feed processing, one for successful
outcomes and the other for failures. Interested applications will subscribe an SQS
queue to the topic and get a copy of the message.

The format for the VIP_DP_AWS_REGION should be like `us-east-1`.

Running it is as per usual for docker-compose:

```sh
$ docker-compose build
$ docker-compose up
```

While it's running, you can place messages on the SQS queue specified by
`VIP_DP_SQS_QUEUE` and it will be picked up by the processor, downloading the
file in the message, the file will be processed, the database populated, the XML
output zipped up and placed in the bucket specified by
`VIP_DP_S3_PROCESSED_BUCKET`, and a message sent the appropriate SNS topic
depending on success/failure. While it's running, you can access the PostgreSQL
server running within Docker on port `55432`.

The message you send on the SQS queue must look like the following:

```clj
{:filename "some-feed.xml"
 :bucket "some-s3-bucket-name"}
```

These two values let `data-processor` know where to find the file and what name
it has.

## Testing

### Non-Postgres tests

`lein test`

### Postgres tests

There are two ways you can do this.

#### Local DB

Using a locally running postgres db
`lein test :postgres`

#### Docker

Will create a DB as part of the docker-compose stack
`script/postgres-tests`

## Developing

The processing pipeline is defined as a sequence of functions that
take and return a processing context. The processing context is a map
containing an `:input`, processing functions in `:pipeline`, and
validation failures in `:warnings`, `:errors`, `:critical`, and
`:fatal`.

### The Processing Pipeline

`run-pipeline` function in the `vip.data-processor.pipeline` namespace
takes a context and will thread that context through each function
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

## Separated Pipelines

Prior versions of the codebase had one basic pipeline that jumped around from namespace
to namespace and sometimes branched based on feed format (xml/csv) and/or spec version,
with the branches then concatenating new steps into the pipeline. This was often
confusing to follow, and it also encouraged some aspects of the context to pull double
duty, being treated differently based on which format and spec version was currently
being processed. The prime example of this is the `:input` keyword, which at first held
the EDN message that kicks off processing, then the zip/xml file, then the extracted
file contents (or if it was a plain XML file, it got wrapped into a vector so that
it had the same collection mechanics that the collection of CSV files does).

The new pipelines are more explicit. There's a common section that all of the pipelines
run to process the EDN message, download the file from S3, validate some file aspects
(like maximum uncompressed size, weeding out files with irrelevant extensions), and lastly
determining which format and spec the feed is in, and then placing the files in a
format appropriate location in the context (`:xml-source-file-path` and
`:csv-source-file-paths`). No more is the XML file the first in a collection of files, which
presumably only holds the one file. It's now the only file path at `:xml-source-file-path`.
And rather than `:input` being mutated over and over again from step to step in the common
pipeline, there are separate keywords in the context for holding `:filename`, `:file`,
`:extracted-file-paths`, `:valid-file-paths`, and then lastly the source file paths mentioned
above. Soon we'll add some specs to pre-validate calls to the pipeline functions that expect
the context to have a certain shape on entry and post-validate additions to the context.

Once the feed format and spec have been determined and the source files put into their
appropriate places in the context, then there's one jump to the pipeline for that
particular combination of format and spec. You can find these pipelines under the
namespaces `vip.data-processor.pipelines.xml` and `vip.data-processor.pipelines.csv`, both
of which then have namespaces `v3` and `v5`. In these namespace you can see all/most of the
steps for that particular flavor of pipeline laid out, rather than needing to trace
execution from namespace to namespace. There are still a few sub-pipelines that collect
a lot of the validations out there that are concatenated in, but in general it should be much
easier to follow, and to make changes to a specific pipeline.

## Deploying

`data-processor` is currently deployed as a [CoreOS cluster][CoreOS] running
atop [AWS EC2][EC2] instances. There are two ways code can be deployed.

### Automatic Deploys

The most common way of deploying new code is by merging an approved PR into the
`master` branch. [Buildkite][buildkite], a continuous integration services,
monitors this repository and triggers a build when code is pushed. For most
branches, the build consists of running a test suite. Code pushed to the
`master` branch - usually by merging an approved pull request - runs the test
suite, and if that is successful, builds a Docker image and deploys it to the
production CoreOS cluster.

### Manual Deploys

In order to manually deploy something other than the `master` branch's `HEAD`,
e.g, to test new features in a staging environment, you'll need to have a few
things:

- `fleetctl` installed (available in Homebrew)
- The public IP address of an EC2 instance in your chosen cluster (aka
  `$FLEETCTL_TUNNEL`), and the appropriate key for that environment added to your ssh-agent, e.g. `ssh-add ~/.ssh/key.pem`.
- A [quay.io account][quay] to use with Docker (run `docker login quay.io` if you haven't
  already)

Begin by checking out the code you want to deploy at a given commit, then run
`script/build`. If the test suite passes, a Docker image is built. Push this
image to Quay as directed by the output of the build script.

Setup your environment for fleet and run `script/deploy`. A successful
deployment should look something like this.

```
$ export FLEETCTL_TUNNEL=an-ip-in-your-cluster
$ ./script/deploy
Agent pid 88741
Destroyed data-processor@.service
Unit data-processor@.service inactive
--- (re-)starting fleet service instances
Destroyed data-processor@1.service
Unit data-processor@1.service inactive
Unit data-processor@1.service launched on 94554086.../10.0.103.12
Destroyed data-processor@2.service
Unit data-processor@2.service inactive
Unit data-processor@2.service launched on 6f2818e4.../10.0.101.122
Destroyed data-processor@3.service
Unit data-processor@3.service inactive
Unit data-processor@3.service launched on cea44c01.../10.0.104.31
```

You can verify that a node is running properly by checking its logs.

```sh
$ fleetctl journal -f data-processor@1
Feb 20 19:04:08 ip-10-0-103-12 bash[25140]: 19:04:08.339 [main] INFO  vip.data-processor - VIP Data Processor starting up. ID: #uuid "9cc1b957-eb61-4e1d-883d-39fffda98967"
Feb 20 19:04:08 ip-10-0-103-12 bash[25140]: 19:04:08.422 [main] INFO  vip.data-processor.db.postgres - Initializing Postgres
Feb 20 19:04:08 ip-10-0-103-12 bash[25140]: Migrating #ragtime.jdbc.SqlDatabase{:db-spec {:connection-uri jdbc:postgresql://your-db-host:5432/database?user=vip&password=secret-secret}, :migrations-table ragtime_migrations, :url jdbc:postgresql://your-db-host:5432/database?user=vip&password=secret-secret, :connection-uri jdbc:postgresql://your-db-host:5432/database?user=vip&password=secret-secret, :db {:type :sql, :url jdbc:postgresql://your-db-host:5432/database?user=vip&password=secret-secret}, :migrator resources/migrations}
Feb 20 19:04:09 ip-10-0-103-12 bash[25140]: 19:04:09.296 [clojure-agent-send-off-pool-0] INFO  squishy.core - Consuming SQS messages from https://sqs.us-east-1.amazonaws.com/1234/your-sqs-queue
```

[CoreOS]: https://coreos.com/
[EC2]: https://aws.amazon.com/ec2/
[buildkite]: https://buildkite.com/the-voting-information-project
[quay]: http://quay.io/

## Database Maintenance

`cleanup.feeds_by_election` looks at any election that has more than 5 copies in the database and returns a list of `old_runs` and a list of `new_runs`.

To delete any but the most recent 5 versions of a feed (where two feeds are considered the "same" if they have the same
`state`, `vip_id`, `election_date`, and `election_type`) run

```sql
delete from results where id in (select unnest(old_runs) from cleanup.feeds_by_election);
```

## Migrations

For development, there are a couple of options for manually running, rolling back, and creating migrations. We now have aliases setup so you can activate these from the command line, or you can also run them from a REPL in the `dev.core` namespace.

### Command Line Migration Interactions

#### Running pending migrations
`lein migrate`

#### Seeing if there are pending migrations
`lein pending`

#### Creating new migration up/down files
`lein create 01-my-cool-new-migration`

With this one, the joplin subsystem automatically prepends the date in YYYYMMDD- format to the value you supply, and creates up/down migration files. If you have multiple migrations on the same day that you want applied in a particular order, our best practice is to start your migrations with 01-, 02-, 03-. That way when the date is prepended, the migrations lexically sort with the 01 first, then 02, 03, etc.

#### Rolling back migrations
Rollback the most recent migration
`lein rollback`

Rollback N number of migrations
`lein rollback N` (where N is some integer between 1 and the total number of migrations applied)

Rollback all migrations after a particular ID
`lein rollback 20210615-01-my-cool-new-migration`
(Note that this is the full ID including the date stamp, but minus the up/down parts)

### REPL
Alternatively to the command line options above, you can call the functions in the `dev.core` namespace directly. The following examples assume you're in the `dev.core` namespace, which is the default when you fire up a REPL.

#### Run all pending migrations
`(migrate)`

#### List all pending migrations
`(pending)`

#### Create a new migration
See the notes about naming migrations under the command line section.
`(create "01-my-cool-new-migration")`

#### Rollback migrations
Rollback the last migration
`(rollback)`

Rollback the last N migrations (where N is an integer between 1 and the number of applied migrations)
`(rollback N)`

Rollback all migrations applied after a specific migration ID (see notes above in command line section)
`(rollback "20210605-01-my-cool-new-migration")`
