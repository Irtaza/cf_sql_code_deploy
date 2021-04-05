import logging
# Set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class AthenaManager:
    def __init__(self, athena_conn):
        '''
        Athena Manager class
        :param athena_conn: Connection object for Athena
        '''
        '''Athena Manager class'''
        self.athena_conn = athena_conn


    def exec_sql_on_athena(self, sql_command, test_output_location, database_name='default'):
        '''
        Executes a single query on Athena then calls get_query_status to get the result of the query
        :param sql_command: The SQL query statements to be executed
        :param test_output_location:The location in S3 where query results are stored
        :param database_name: The database within which the query executes, default is 'default'
        :return: Returns a dictionary object with information about the query exewcution including QueryExecutionId, Query, Status, Failure Reason, if any.
        '''
        response = self.athena_conn.start_query_execution(
            QueryString=sql_command,
            QueryExecutionContext={
                'Database': database_name
            },
            ResultConfiguration={
                'OutputLocation': test_output_location
            }
        )

        response_status = 'RUNNING'
        while response_status == 'RUNNING' or response_status == 'QUEUED':
            response = self.get_query_status(response['QueryExecutionId'])
            response_status = response['Status']

        if response_status == 'FAILED' or response_status == 'CANCELLED':
            raise Exception(response)

        return response



    def get_query_status(self, query_execution_id):
        '''
        Returns information about a single execution of a query. Each time a query executes, information about the query execution is saved with a unique ID.
        :param query_execution_id:
        :return: Returns a dictionary object with information about the query execution including QueryExecutionId, Query, Status, Failure Reason, if any.
        '''
        response = self.athena_conn.get_query_execution(QueryExecutionId=query_execution_id)

        response_data = {'QueryExecutionId': response['QueryExecution']['QueryExecutionId'],
                         'Query': response['QueryExecution']['Query'],
                         'Status': response['QueryExecution']['Status']['State']
                         }

        if 'StateChangeReason' in response['QueryExecution']['Status']:
            response_data.update({'StateChangeReason': response['QueryExecution']['Status']['StateChangeReason']})

        return response_data
