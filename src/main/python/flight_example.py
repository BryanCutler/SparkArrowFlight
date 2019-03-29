import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
from pyspark.sql.types import to_arrow_schema

from spark_flight_connector import SparkFlightConnector
from tensorflow_flight_dataset import ArrowFlightDataset


def start_flight_service(port):
    proc = subprocess.Popen([
        'java',
        '-cp', 'target/FlightConnector-1.0-SNAPSHOT-jar-with-dependencies.jar',
        'bryan.SparkFlightServer',
        '--port', str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc


def main():

    # Location of the Flight Service
    host = '127.0.0.1'
    port = 8888

    # Unique identifier for flight data
    flight_desc = 'spark-flight-descriptor'

    # ------------------------ #
    # Start the Flight Service #
    # ------------------------ #
    server_proc = start_flight_service(port=8888)

    # --------------------------------------------- #
    # Run Spark to put Arrow data to Flight Service #
    # --------------------------------------------- #
    spark = SparkSession \
        .builder \
        .appName('spark-flight') \
        .getOrCreate()

    df = spark.range(10) \
        .select((col('id') % 2).alias('label')).withColumn('data', rand())

    SparkFlightConnector.put(df, host, port, flight_desc)

    # ------------------------------------------------------------- #
    # Create tf.data.Dataset to iterate over Arrow data from Flight #
    # ------------------------------------------------------------- #
    dataset = ArrowFlightDataset.from_schema(host, port, flight_desc, to_arrow_schema(df.schema))

    it = dataset.make_one_shot_iterator()
    n = it.get_next()

    import tensorflow as tf
    with tf.Session() as sess:
        while True:
            try:
                result = sess.run(n)
                print(result)
            except tf.errors.OutOfRangeError:
                break

    # Cleanup
    spark.stop()
    dataset.proc.terminate()
    server_proc.terminate()


if __name__ == "__main__":
    main()
