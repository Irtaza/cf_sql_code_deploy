import unittest
from aws import Connection
from aws import AthenaManager
import sql_deploy_custom_resource_lambda as s


class TestSqlDeployCustomResourceLambda(unittest.TestCase):

    def setUp(self):
        """Setup"""
        test_name = self.shortDescription()

        # Instantiate Connection
        self.conn = Connection()
        # Create an Athena connection
        self.conn_athena = self.conn.athena_connection()

        # Instantiate Athena Manager
        self.athena_manager = AthenaManager(self.conn_athena)

        self.test_ouput_location = 's3://aws-athena-query-results-933886674506-eu-west-1/'

        if test_name == "Test routine get_sql_commands_from_file":
            self.test_list_of_commands = ['CREATE DATABASE IF NOT EXISTS unittest', 'DROP DATABASE IF EXISTS unittest']

        if test_name == "Test routine replace_placeholders_in_sql":
            self.test_list_of_commands = ['CREATE __client__ IF NOT EXISTS __bucket__', 'DROP DATABASE IF EXISTS __client_____bucket__']
            self.test_dict_of_replacements = {'__bucket__':'unittest', '__client__':"batman"}
            self.test_list_of_updated_commands = ['CREATE batman IF NOT EXISTS unittest', 'DROP DATABASE IF EXISTS batman_unittest']

    def test_get_sql_commands_from_file(self):
        """Test routine get_sql_commands_from_file"""
        list_of_commands = s.get_sql_commands_from_file('test_split.txt' ,';')
        self.assertEqual(self.test_list_of_commands, list_of_commands, "Lists of commands don't match")

    def test_execute_scripts_with_create_event(self):
        """Test routine execute_scripts"""
        dict_of_responses = s.execute_scripts(self.athena_manager, 'test_split.txt', self.test_ouput_location, 'Create' )
        self.assertTrue(dict_of_responses['Create'][0]['Status']=='SUCCEEDED', 'First command failed.')
        self.assertTrue(dict_of_responses['Create'][1]['Status'] == 'SUCCEEDED', 'Second command failed.')

    def test_execute_scripts_with_delete_event(self):
        """Test routine execute_scripts"""
        dict_of_responses = s.execute_scripts(self.athena_manager, 'test_split.txt', self.test_ouput_location, 'Delete' )
        self.assertTrue(dict_of_responses['Delete'][0]['Status']=='SUCCEEDED', 'First command failed.')
        self.assertTrue(dict_of_responses['Delete'][1]['Status'] == 'SUCCEEDED', 'Second command failed.')

    def test_replace_placeholders_in_sql(self):
        """Test routine replace_placeholders_in_sql"""
        list_of_updated_commands = s.replace_placeholders_in_sql(self.test_list_of_commands, self.test_dict_of_replacements)
        self.assertEqual(list_of_updated_commands, self.test_list_of_updated_commands, "The placeholder replacement failed.")


