# log-reader

Serverless Cloudfront log reader

## overview

This Lambda function is meant to be triggered by a `PutObject` event on S3 from the Cloudfront logging service. The function reads in the log file and parses it. Next it writes the select records and data to a postgres database.

## development

### install

`npm install`

### setup

Use `aws configure` and set the proper access key and secret on your local machine.

### run tests

`npm run test`

## deployment

### build

`docker build -t log-reader .`

### tag

`docker tag log-reader <aws-account-id>.dkr.ecr.<region>.amazonaws.com/log-reader:latest`

### push

`docker push <aws-account-id>.dkr.ecr.<region>.amazonaws.com/log-reader:latest`

Guide: https://docs.aws.amazon.com/lambda/latest/dg/nodejs-image.html#nodejs-image-instructions

NOTE: If building on an M1 Mac the architecture on the Lambda function should be set to `arm64`.
