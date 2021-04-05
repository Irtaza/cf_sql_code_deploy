import unittest
import aws
import boto3
from moto import mock_s3

class TestS3Manager(unittest.TestCase):
    mock_s3 = mock_s3()

    def setUp(self):
        self.mock_s3.start()
        self.location = "eu-west-1"
        self.bucket_name = 'test_bucket_01'
        self.key_name = 'stats_com/fake_fake/test.json'
        self.key_contents = 'This is test data.'
        s3 = boto3.resource('s3', region_name=self.location)
        bucket = s3.create_bucket(Bucket=self.bucket_name)
        s3.Object(self.bucket_name, self.key_name).put(Body=self.key_contents)
        self.conn = aws.Connection().s3_connection()
        self.s3_manager = aws.S3Manager(self.conn)


    def tearDown(self):
        self.mock_s3.stop()


    def test_download_object(self):
        s3 = boto3.resource('s3', region_name=self.location)
        bucket = s3.Bucket(self.bucket_name)
        assert bucket.name == self.bucket_name

        downloaded_key = self.s3_manager.download_object(self.bucket_name, self.key_name)
        print(downloaded_key)
        self.assertEqual(downloaded_key, '/tmp/{}'.format(self.key_name.replace('/','$$$')), 'File not downloaded properly')
        # retrieve already setup keys
        '''keys = list(bucket.objects.filter(Prefix=self.key_name))
        assert len(keys) == 1
        assert keys[0].key == self.key_name
        # update key
        s3.Object(self.bucket_name, self.key_name).put(Body='new')
        key = s3.Object(self.bucket_name, self.key_name).get()
        assert 'new' == key['Body'].read()'''