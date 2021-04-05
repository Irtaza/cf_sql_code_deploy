import unittest
import aws
import boto3
from moto import mock_s3
from moto import mock_emr


class TestConnection(unittest.TestCase):


    def setUp(self):
        """Setup"""
        test_name = self.shortDescription()
        if test_name != "Test routine there_is_a_aws_connection":
            self.conn = aws.Connection()


    def test_there_is_a_aws_connection(self):
        """Test routine there_is_a_aws_connection"""
        conn = aws.Connection()
        self.assertIsInstance(conn, aws.Connection, 'The object is not an instance of class Connection')


    def test_there_is_a_s3_resource_object(self):
        """Test routine there_is_a_s3_resource_object"""
        s3 = boto3.resource('s3')
        s3_conn = self.conn.s3_connection()
        self.assertEqual(s3_conn, s3, 'The object is not a s3 resource')

    @mock_s3
    def test_there_is_an_athena_connection_object(self):
        """Test routine there_is_an_athena_connection_object"""
        athena_conn = self.conn.athena_connection()
        self.assertEqual(str(type(athena_conn)), "<class 'botocore.client.Athena'>", 'The object is not an athena object')


    @mock_emr
    def test_there_is_an_emr_connection_object(self):
        """Test routine there_is_an_emr_connection_object"""
        emr_conn = self.conn.emr_connection()
        self.assertEqual(str(type(emr_conn)), "<class 'botocore.client.EMR'>", 'The object is not an emr client')



if __name__ == '__main__':
    unittest.main()
