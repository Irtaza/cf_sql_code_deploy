import boto3


class Connection:

    def __init__(self):
        '''Connection Instance'''


    def s3_connection(self):
        '''Create and return an EC2 connection'''
        s3 = boto3.resource('s3')
        return s3


    def emr_connection(self):
        '''Create and return an EMR connection'''
        emr = boto3.client('emr')
        return emr


    def athena_connection(self):
        '''Create and return an Athena connection'''
        athena = boto3.client('athena')
        return athena