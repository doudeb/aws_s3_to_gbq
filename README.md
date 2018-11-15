# aws_s3_to_gbq
An aws lambda handler to store a data file to google big query

## Why
Whith this script you wil be able to move datas from a CSV or TSV file from a s3 datalake to Big Query and then stream it as your wishs.

## How
1. Create a running local copy
```
clone git@github.com:doudeb/aws_s3_to_gbq.git
cd aws_s3_to_gbq
virtualenv .
source bin/activate
pip install -r requirements.txt
```
2. Create a Google json api key, name it auth.json and place it in the root directory
*More information can be found here https://cloud.google.com/docs/authentication/getting-started for the api key generation.*

3. Test it
You can run localy with `./stream_to_bq.py --file_name file.txt --project_id project --dataset_id dataset --table_name table`
**It will create the table if not exists**

4. Define a primary key
You must add the pattern `primary_key` as descritption of your primary key column on your destination table.

## Prepare a lambda package
```
zip -r9 ../../s3tobq.zip *
zip -g s3tobq.zip stream_to_bq.py auth.json
```
## You are now ready to create your lambda package, refer to the aws lambda documentation for it
