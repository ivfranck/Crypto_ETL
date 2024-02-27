import logging
from spark import spark
from extract import extract
from transform import transform
from src.load import load


def logger_setup():
    # logger setup
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    handler = logging.FileHandler("crypto.log")
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def main():
    logger = logger_setup()
    # start spark session
    logger.info("Starting spark session...")
    SPARK_SESSION = spark.Spark("crypto_etl")._SPARK_SESSION

    try:
        # extract data form coingecko API
        Extract = extract.Extract(logger)
        Extract.get_coins_data()

        if (Extract.data):
            logger.info("API data fetch successful")
            # transform data
            Transform = transform.Transform(Extract.data, SPARK_SESSION, logger)
            Transform.transform_data()

        else:
            logger.info("Transformation suspended. No data to Transform")

        if (Transform.new_data):
            logger.info("Transformation successful")

            # load the data to aws s3
            Load = load.Load(Transform.new_data, logger)
            Load.write_dataframe_to_s3()
        else:
            logger.info("S3 Load suspended. No data to Load")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        SPARK_SESSION.stop()


if __name__ == '__main__':
    main()
