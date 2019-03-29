
class SparkFlightConnector(object):

    @staticmethod
    def put(dataframe, host, port, descriptor):
        sc = dataframe._sc
        jconn = sc._jvm.bryan.SparkFlightConnector()
        jconn.put(dataframe._jdf, host, port, descriptor)
