import pandas as pd
from src.schema import schema


class Transform:

    def __init__(self, data, spark_session, logger=None):
        self.SPARK_SESSION = spark_session
        self.logger = logger
        self.data = data
        self.new_data = None

    def transform_data(self):

        try:
            self.logger.info(f"Transforming data...")
            # convert data (list of objs): Any to pandas DF
            pd_df = pd.DataFrame(self.data)

            df = self.converto_to_spark_df(pd_df)
            self.logger.info(f"{df.count()} rows | {len(df.columns)} columns added to DataFrame")
            self.new_data = df

            self.new_data.show(10)
        except Exception as e:
            self.logger.error(f"Transformation failed. Error: {e}")

    def converto_to_spark_df(self, df):
        pd_data_df = df.drop("roi", axis=1)
        # convert pandas DF to spark DF
        sp_df = self.SPARK_SESSION.createDataFrame(pd_data_df, schema=schema.schema)

        return sp_df
