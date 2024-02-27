from src.aws_credentials import _AWS_ACCESS_KEY, _AWS_SECRET_KEY
from pyspark.sql import SparkSession


class Spark:
    def __init__(self, app_name):
        self._SPARK_SESSION = (SparkSession.builder.appName(app_name)
                               .config("spark.hadoop.fs.s3a.access.key", _AWS_ACCESS_KEY)
                               .config("spark.hadoop.fs.s3a.secret.key", _AWS_SECRET_KEY)
                               .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.0")
                               .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a'
                                                                                       '.SimpleAWSCredentialsProvider')
                               .getOrCreate())
