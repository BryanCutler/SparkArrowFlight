
class SparkFlightConnector(object):

    @staticmethod
    def put(dataframe, host, port, descriptor):
        sc = dataframe._sc
        jconn = sc._jvm.com.ibm.codait.SparkFlightConnector()
        jconn.put(dataframe._jdf, host, int(port), descriptor)
