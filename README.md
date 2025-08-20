# Sync Snowflake-Managed Iceberg Tables to AWS Glue

This solution enables querying Snowflake-managed Iceberg tables from Amazon Athena by automatically synchronizing metadata changes to the AWS Glue Data Catalog.

## Support Notice

All code is provided for reference purposes only. Please note that this code is provided “AS IS” and without warranty. Snowflake will not offer any support for use of the sample code.

## Background & Problem Statement

When working with Snowflake-managed Iceberg tables, you may need to query them from Amazon Athena. Athena is basically a shared Trino cluster AWS runs for you and [Trino can query Snowflake-managed Iceberg tables OOTB](https://trino.io/docs/current/object-storage/metastores.html#iceberg-snowflake-catalog). However, AWS has removed that ability in Athena (which uses Trino under the hood) have removed support for querying external catalogs directly. This means Athena can only query tables registered in the glue data catalog.

There is no OOTB support for keeping glue up to date when changes are made to a Snowflake-managed Iceberg table. This repo provides a utility to bridge that gap until a long term / supported solution is available. 

## Solution Overview

This repository provides an automated solution to synchronize Snowflake-managed Iceberg table metadata with AWS Glue. It includes:

1. A Snowflake stream to detect changes to the Iceberg table
2. A Snowflake task that triggers when changes are detected
3. A Snowflake UDF that updates the metadata location in AWS Glue

### How It Works

1. When changes occur to the Iceberg table in Snowflake, they are captured by a stream
2. A Snowflake task monitors the stream and triggers when changes are detected
3. The task gets the current metadata location from Snowflake, then calls a custom function that takes that new metadata location and updates the glue metadata for it to point to this new location
4. Athena can then query the table using the updated metadata from glue

## Important Notes

This solution is intended as a temporary workaround until either:
- Athena puts the Trino capability for querying external catalogs in Athena
- AWS implements catalog federation in Glue

Once either of these long-term solutions becomes available, this synchronization mechanism should no longer be necessary.

### Warning

This solution does not attempt to merge any changes made outside of Snowflake to these Iceberg tables. Therefore, any modifications should be prevented so no other engine/tool can make changes to these tables.
