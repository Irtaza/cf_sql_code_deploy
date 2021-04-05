from __future__ import print_function
import json
from botocore.vendored import requests
from aws import Connection
from aws import S3Manager
from aws import AthenaManager
import logging
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


SUCCESS = "SUCCESS"
FAILED = "FAILED"


def send_response(event, context, responseStatus, responseData, physicalResourceId):
    '''
    Sends response back to the CloudFormation custom resource with teh status of the task.
    :param event: The fields in a custom resource request.
    :param context: An object, specific to Lambda function to get status of the Lambda function execution state
    :param responseStatus: Whether the function successfully completed, SUCCESS or FAILED
    :param responseData: The Data field of a custom resource response object. The data is a list of name-value pairs.
    :param physicalResourceId: Optional. The unique identifier of the custom resource that invoked the function.
    :return: N/A
    '''
    responseUrl = event['ResponseURL']

    logger.info(responseUrl)

    responseBody = {}
    responseBody['Status'] = responseStatus
    responseBody['Reason'] = context.log_stream_name
    responseBody['PhysicalResourceId'] = physicalResourceId or context.log_stream_name
    responseBody['StackId'] = event['StackId']
    responseBody['RequestId'] = event['RequestId']
    responseBody['LogicalResourceId'] = event['LogicalResourceId']
    responseBody['Data'] = responseData

    json_responseBody = json.dumps(responseBody)

    logger.info("Response body:\n" + json_responseBody)


    headers = {
        'content-type': '',
        'content-length': str(len(json_responseBody))
    }

    try:
        response = requests.put(responseUrl,
                                data=json_responseBody,
                                headers=headers)
        logger.info("Status code: " + response.reason)
    except Exception as e:
        logger.info("send_response(..) failed executing requests.put(..): " + str(e))


def get_sql_commands_from_file(file_name, delimiter=';'):
    '''
    Gets list of sql commands from a file that contains list of commands delimited by a delimiter
    and returns them as a list of strings.
    :param file_name: The file that contains the delimiter separated SQL Commands
    :param delimiter: The delimiter used to separate each command.
    :return: A list of strings containing the SQL Commands
    '''
    with open(file_name, 'r') as f:
        content = f.read()
        list_of_commands = content.split(delimiter)

    # Remove empty strings, tuples etc from the list
    filtered_list_of_commands = [commands.strip() for commands in list_of_commands if commands.strip() != '']

    return filtered_list_of_commands


def replace_placeholders_in_sql(source_list, dict_of_replacements):
    '''
    Replaces placeholder values in a list with actual values
    :param source_list: A list that contains list of sql commands with placeholders
    :param list_of_replacements: A list of dictionary items containing placeholder and the desired value
    :return: Updated list of commands with placeholder replaced with actual values
    '''
    updated_list_of_commands = []
    for item in source_list:
        for src, target in list(dict_of_replacements.items()):
            item = item.replace(src, target)
        updated_list_of_commands.append(item)

    return updated_list_of_commands


def execute_scripts(athena_manager, ddl_filename, test_output_location, dict_of_replacements, event_type):
    '''
    Downloads the file contianing SQL Commands; Runs them on Athena and then returns a dictionary of response
    received from Athena
    :param athena_manager: An instance of Athena Manager
    :param s3_manager: An instance of S3 Manager
    :param ddl_bucket: The bucket that contains the SQL Commands
    :param ddl_create_key: The name, including the prefix(es) of the file containing the SQL Commands
    :param test_output_location: The location in S3 where query results are stored
    :param dict_of_replacements: A dictionary of placeholders and their corresponding values
    :param event_type: The event that launches the Lambda function, Create or Delete
    :return: A dictionary containing responses for each SQL command.
    '''

    list_of_sql_commands = get_sql_commands_from_file(ddl_filename, ';')
    logger.info('List of SQL Commds:\n{}'.format(list_of_sql_commands));

    updated_list_of_commands = replace_placeholders_in_sql(list_of_sql_commands, dict_of_replacements)

    list_of_responses = [athena_manager.exec_sql_on_athena(sql_command, test_output_location) for sql_command in
                         updated_list_of_commands]
    logger.info('List of responses from Athena:\n{}'.format(list_of_responses))

    return {event_type: list_of_responses}


def lambda_handler(event, context):
    '''
    The entry point for the Lambda function
    :param event: A dictionary containing all the information about the event that triggered the Lambda function
    :param context: This object contains information about the state of the Lambda function itself.
    :return: N/A
    '''
    # set environment variables
    environment_name = event['ResourceProperties']['EnvnameProperty']
    micro_environment= event['ResourceProperties']['MicroenvProperty']
    client= event['ResourceProperties']['ClientProperty']
    account= event['ResourceProperties']['AccountProperty']
    ddl_bucket = event['ResourceProperties']['DDLBucketProperty']
    ddl_create_key = event['ResourceProperties']['CreateSqlS3KeyProperty']
    ddl_delete_key = event['ResourceProperties']['DeleteSqlS3KeyProperty']
    data_bucket = event['ResourceProperties']['DataBucketProperty']
    test_output_location = event['ResourceProperties']['AthenaSqlOutputUriProperty']
    physical_resource_id = context.log_stream_name

    # Create a dictionary of placeholders and their values that will be replaced in sql commands.
    dict_of_replacements = {"__bucket__":data_bucket,
                             "__envname__": environment_name,
                             "__microenv__": micro_environment,
                             "__client__": client,
                             "__account__": account
                             }

    ###Set AWS specific variables###
    # Instantiate Connection
    conn = Connection()
    # Create an Athena connection
    conn_athena = conn.athena_connection()
    # Create an S3 connection
    conn_s3 = conn.s3_connection()
    # Instantiate S3Manager
    s3_manager = S3Manager(conn_s3)

    # Instantiate Athena Manager
    athena_manager = AthenaManager(conn_athena)


    try:
        if event['RequestType'] != 'Delete':
            # Download file that has the SQL Create Commands from s3
            ddl_filename = s3_manager.download_object(ddl_bucket, ddl_create_key)
            logger.info('Downloaded {}'.format(ddl_filename))

            response = execute_scripts(athena_manager, ddl_filename, test_output_location, dict_of_replacements, 'Create')
            send_response(event, context, SUCCESS, response, physical_resource_id)
            return response

        elif event['RequestType'] == 'Delete':
            # Download file that has the SQL Delete Commands from s3
            ddl_filename = s3_manager.download_object(ddl_bucket, ddl_delete_key)
            logger.info('Downloaded {}'.format(ddl_filename))

            response = execute_scripts(athena_manager, ddl_filename, test_output_location, dict_of_replacements, 'Delete')
            send_response(event, context, SUCCESS, response, physical_resource_id)
            return response

    except Exception as error:
        logger.info(error.args)
        response_data = {'Reason': error.args}
        send_response(event, context, FAILED, response_data, physical_resource_id)
        return error.args
