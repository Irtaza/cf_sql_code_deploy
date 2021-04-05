import uuid
import logging

# Set log level
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class S3Manager:

    def __init__(self, s3_resource):
        '''
        s3 Manager class
        :param s3_connection: S3 resource object
        '''
        self.s3_resource = s3_resource

    def download_object(self, src_bucket_name, src_file_name, dest_directory='/tmp/', add_uuid=False):
        '''
        Downloads a file from S3 to local directory
        :param s3_resource: An instance of S3 connection object from Connection class
        :param src_bucket_name: The name of the S3 bucket that contains the file
        :param src_file_name: The name of the file to download including any prefixes
        :param dest_directory: The lcoal directory where the file is to be downloaded - default is '/tmp'
        :param add_uuid: Optional argument, if true a UUID value is added to the name of the downloaded file
        :return: Returns the URI of the downloaded file
        '''
        logger.info("Downloading s3://{}/{} to {}".format(src_bucket_name, src_file_name, dest_directory))
        if (add_uuid):
            dest_file_name = dest_directory + src_file_name.replace('/','$$$') + '_' + uuid.uuid4().hex
        else:
            dest_file_name = dest_directory + src_file_name.replace('/','$$$')
        bucket = self.s3_resource.Bucket(src_bucket_name)
        bucket.download_file(src_file_name, dest_file_name)

        return dest_file_name


    def upload_object(self, src_file_path, src_file_name, dest_bucket_name, dest_file_path, dest_file_name):
        '''
        Uploads a local file to S3
        :param conn: An instance of S3 connection object from Connection class
        :param src_file_path: The directory name where the local file is stored
        :param src_file_name: Name of the file to be uploaded
        :param dest_bucket_name: The name of the S3 bucket where the file is to be uploaded
        :param dest_file_path: The prefix value where the file is to be uploaded within the bucket
        :param dest_file_name: The name of the file to be uploaded
        :return: N/A
        '''
        logger.info("Uploading {}/{}".format(src_file_path, src_file_name))
        self.s3_resource.Object(dest_bucket_name, '{}/{}'.format(dest_file_path, dest_file_name)).put(
            Body=open('{}/{}'.format(src_file_path, src_file_name), 'rb'))


    def delete_object(self, bucket_name, file_path, file_name):
        '''
        Removes a files from S3
        :param conn: An instance of S3 connection object from Connection class
        :param bucket_name: The name of the S3 bucket that contains the file
        :param file_path: The prefix value where the file is stored
        :param file_name: The name of the file to be deleted
        :return: N/A
        '''
        logger.info("Deleting {}/{}".format(file_path, file_name))
        self.s3_resource.Object(bucket_name, '{}/{}'.format(file_path, file_name)).delete()

