from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("image", StringType(), True),
        StructField("current_price", FloatType(), True),
        StructField("market_cap", LongType(), True),
        StructField("market_cap_rank", IntegerType(), True),
        StructField("fully_diluted_valuation", StringType(), True),
        StructField("total_volume", LongType(), True),
        StructField("high_24h", FloatType(), True),
        StructField("low_24h", FloatType(), True),
        StructField("price_change_24h", FloatType(), True),
        StructField("price_change_percentage_24h", FloatType(), True),
        StructField("market_cap_change_24h", FloatType(), True),
        StructField("market_cap_change_percentage_24h", FloatType(), True),
        StructField("circulating_supply", FloatType(), True),
        StructField("total_supply", StringType(), True),
        StructField("max_supply", StringType(), True),
        StructField("ath", FloatType(), True),
        StructField("ath_change_percentage", FloatType(), True),
        StructField("ath_date", StringType(), True),
        StructField("atl", FloatType(), True),
        StructField("atl_change_percentage", FloatType(), True),
        StructField("atl_date", StringType(), True),
        StructField("last_updated", StringType(), True),
    ]
)
