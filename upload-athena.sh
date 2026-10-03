#!/bin/sh
echo Uploading gcd-parquet/snapshot\=$1
aws s3 cp --recursive gcd-parquet/snapshot\=$1 s3://gcd-parquet/snapshot\=$1/
