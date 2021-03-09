import time
import subprocess


def start_flight_to_stream(host, port, flight_descriptor):
    proc = subprocess.Popen([
        'java',
        '-cp', 'target/SparkFlightConnector-1.0-SNAPSHOT.jar',
        'com.ibm.codait.FlightToStream',
        '--host', host,
        '--port', str(port),
        '--stream-port', '8889',
        '--flight-descriptor', flight_descriptor],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(3)  # wait for server to start
    return proc


class ArrowFlightDataset(object):

    @staticmethod
    def from_schema(host, port, flight_descriptor, arrow_schema):
        import tensorflow as tf
        import tensorflow_io.arrow as arrow_io

        proc = start_flight_to_stream(host, port, flight_descriptor)

        dataset = arrow_io.ArrowStreamDataset.from_schema('127.0.0.1:8889', arrow_schema)

        dataset.proc = proc
        return dataset
