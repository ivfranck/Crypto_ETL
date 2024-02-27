import json
import unittest
import pandas as pd
from pyspark.testing import assertDataFrameEqual
import pyarrow
from pyspark import SparkConf, SparkContext
from src.transform import transform
from src.spark import spark
from src.schema import schema


class TestDataTransformation(unittest.TestCase):
    def test_transform_data(self):
        # start spark session
        spark_instance = spark.Spark("transform_test")
        spark_session = spark_instance._SPARK_SESSION

        data = pd.read_json("../testData/test_data.json").head(6)
        transform_instance = transform.Transform(data=data, spark_session=spark_session)
        df = transform_instance.converto_to_spark_df(data)
        df.show()

        expected_data = pd.read_json("../testData/expected_test_data.txt")
        expected_df = spark_session.createDataFrame(expected_data, schema=schema.schema)
        expected_df.show()

        assertDataFrameEqual(df, expected_df)
