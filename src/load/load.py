import boto3
from datetime import datetime
from src.aws_credentials import _AWS_ACCESS_KEY, _AWS_SECRET_KEY


class Load:
    def __init__(self, data, logger):
        self.logger = logger
        self.data = data
        self.S3_BUCKET = "crypto-etl"
        self.SERVICE_NAME = "s3"
        self.REGION_NAME = "eu-central-1"
        self.AWS_ACCESS_KEY_ID = _AWS_ACCESS_KEY
        self.AWS_SECRET_ACCESS_KEY = _AWS_SECRET_KEY

    def s3_connection(self):
        try:
            s3 = boto3.resource(
                service_name=self.SERVICE_NAME,
                region_name=self.REGION_NAME,
                aws_access_key_id=self.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY
            )

            # print all s3 buckets
            for bucket in s3.buckets.all():
                print(bucket.name)

        except Exception as e:
            self.logger.error(f"Failed writing to S3 bucket. Error: {e}")

    def write_dataframe_to_s3(self):
        # generate today's date and time as a string
        current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # specify S3 path with the dynamically generated folder name
        s3_path = f"s3a://{self.S3_BUCKET}/{current_datetime}/"
        self.logger.info(f"Path to S3 bucket: {s3_path}")

        try:
            # write the DataFrame to S3 in Parquet format
            self.logger.info(f"Writing to S3 bucket...")
            self.data.write.parquet(s3_path, mode="overwrite")
        except Exception as e:
            self.logger.error(f"Failed writing to S3 bucket. Error: {e}")


