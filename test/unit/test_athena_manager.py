import unittest
from aws import Connection
from aws import AthenaManager

class TestAthenaManager(unittest.TestCase):

    def setUp(self):
        """Setup"""
        test_name = self.shortDescription()

        # Instantiate Connection
        self.conn = Connection()
        # Create an Athena connection
        self.conn_athena = self.conn.athena_connection()

        # Instantiate Athena Manager
        self.athena_manager = AthenaManager(self.conn_athena)

        self.test_ouput_location = 's3://aws-athena-query-results/'
        self.success_query = 'CREATE DATABASE IF NOT EXISTS unittest'
        self.fail_query = 'DROP DATABASE unittest'

        if test_name == "Test routine get_sql_commands_from_file":
            self.test_list_of_commands = ['CREATE DATABASE IF NOT EXISTS unittest', 'DROP DATABASE IF EXISTS unittest']


    def test_exec_sql_on_athena_response_contains_correct_keys(self):
        """Test routine exec_sql_on_athena_response_contains_correct_keys"""
        response = AthenaManager.exec_sql_on_athena(self.athena_manager, self.success_query, self.test_ouput_location)
        self.assertTrue('Status' in response and 'QueryExecutionId' in response and 'Query' in response, 'Response does not include all required  keys')


    def test_on_success_exec_sql_on_athena_retruns_success_status(self):
        """Test routine on_success_exec_sql_on_athena_retruns_success_status"""
        response = AthenaManager.exec_sql_on_athena(self.athena_manager, self.success_query, self.test_ouput_location)
        self.assertTrue(response['Status'] == 'SUCCEEDED', 'SQL command was not successfull')


    def test_on_failure_exec_sql_on_athena_raises_exception(self):
        """Test routine on_failure_exec_sql_on_athena_raises_exception"""
        self.assertRaises(Exception, AthenaManager.exec_sql_on_athena(self.athena_manager, self.fail_query, self.test_ouput_location))

