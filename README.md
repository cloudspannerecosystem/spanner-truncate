spanner-truncate
===
[![CircleCI](https://circleci.com/gh/cloudspannerecosystem/spanner-truncate.svg?style=svg)](https://circleci.com/gh/cloudspannerecosystem/spanner-truncate)

## Overview

spanner-truncate is a tool to delete all rows from the tables in a Cloud Spanner database without deleting tables themselves.

Please feel free to report issues and send pull requests, but note that this application is not officially supported as part of the Cloud Spanner product.

## Use cases

* Delete rows from the database while keeping the underlying splits, which are typically for [database pre-warming before the launch](https://cloud.google.com/solutions/best-practices-cloud-spanner-gaming-database#pre-warm_the_database_before_launch).
* Delete rows from the database without requiring strong IAM permissions for deleting tables or databases.

## Motivation

At a glance deleting all rows from the database looks an easy task, but there are several issues we could encounter when we want to delete rows from the real-world databases.

* If the table size is huge, a simple DELETE statement like `DELETE FROM table WHERE true` could easily exceed the [transaction mutation limit](https://cloud.google.com/spanner/quotas).
* Rows in interleaved tables which have `PARENT ON DELETE NO ACTION` must be deleted first before deleting the rows from the parent table, otherwise it will cause a constraint violation error.
* Rows in the tables which reference other tables with `FOREIGN KEY` constraints must be deleted first before deleting rows in the referenced tables, otherwise it will cause a constraint violation error.
* It would take a lot of time if we delete rows from the tables one by one.

## How this tool works

To solve the preceding issues, this tool works as follows.

* Use [Partitioned DML](https://cloud.google.com/spanner/docs/dml-partitioned) to delete all rows from the table to overcome the single transaction mutation limit.
* Delete rows from multiple tables in parallel to minimize the total time for deletion.
* Automatically discover the constraints between tables and delete rows from the tables in proper order without violating database constraints.

## Limitations

* This tool does not guarantee the atomicity of deletion. If you access the rows that are being deleted, you will get the inconsistent view of the database.
* This tool does not delete rows which were inserted while the tool was running.

## Install

```
go get -u github.com/cloudspannerecosystem/spanner-truncate
```

## How to use

```
Usage:
  spanner-truncate [OPTIONS]

Application Options:
  -p, --project=  (required) GCP Project ID.
  -i, --instance= (required) Cloud Spanner Instance ID.
  -d, --database= (required) Cloud Spanner Database ID.
  -q, --quiet     Disable all interactive prompts.

Help Options:
  -h, --help      Show this help message
```

Example:

```
$ spanner-truncate -p myproject -i myinstance -d mydb
Fetching table information from projects/myproject/instances/myinstance/databases/mydb
Albums
Concerts
Singers
Songs

Rows in these tables will be deleted. Do you want to continue? [Y/n] Y
Concerts: completed    13s [============================================>] 100% (1,200 / 1,200)
Singers:  completed    13s [============================================>] 100% (6,000 / 6,000)
Albums:   completed    12s [============================================>] 100% (1,800 / 1,800)
Songs:    completed    11s [============================================>] 100% (3,600 / 3,600)

Done! All rows have been deleted successfully.
```
