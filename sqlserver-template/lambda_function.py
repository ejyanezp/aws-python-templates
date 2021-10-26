import pyodbc
import datetime
import json
import boto3
import os
from ccb_toolbox import ccb_global
from ccb_toolbox import ccb_sql


# connection_data = dbutils_sql.get_secret(f"/qa/sql/bcredicorp")
connection_data = ccb_global.get_parameters(["/dev/sql/2016/globals", "/dev/sql/2016/bcredicorp"])
conn = ccb_sql.connect(connection_data)
sqs_conn = boto3.client('sqs')
sql = """
DECLARE
    @return_value int,
    @p_response_code varchar(40),
    @p_message varchar(40)
EXEC @return_value = [dbo].[pro_get_creditcard_data]
    @p_format = ?,
    @p_company = ?,
    @p_source = ?,
    @p_country = ?,
    @p_query_date = ?,
    @p_year_month = ?,
    @p_account_id = ?,
    @p_credit_card = ?,
    @p_traceId = ?,
    @p_response_code = @p_response_code OUTPUT,
    @p_message = @p_message OUTPUT

SELECT @p_response_code as N'@p_response_code', 
    @p_message as N'@p_message'
"""


def mock_handler(event, context):
    return {
        "success": True,
        "data": {
            "resultset0": [
                {
                    "balanceactual": "6492.20",
                    "crlim": "5500",
                    "descr": "CREDICORP VISA PLATINUM",
                    "disponible_compras": "0.00",
                    "disponible_retiro_efectivo": "0.00",
                    "fecpagodue": "2020-01-15T00:00:00",
                    "fecultecta": "2019-12-20T00:00:00",
                    "fecultpago": "2019-12-12T00:00:00",
                    "numcuenta": "0000004765340020328",
                    "numtarjeta": "4765340008416734",
                    "pago_minimo": "878.00",
                    "puntos_al_dia": "0",
                    "puntos_ultimo_estado": "0",
                    "query_date": "2021-09-24T23:05:47.160000",
                    "ultpago": "3.38"
                }
            ]
        },
        "traceid": "mock-d79-0786-4b06-844e-766905e77ca7"
    }


def lambda_handler(event, context):
    mock_mode = False if os.environ.get('MOCK_MODE') is None or os.environ.get('MOCK_MODE') == 'FALSE' else True
    if mock_mode:
        return mock_handler(event, context)
    start_clock = datetime.datetime.now()
    if 'trace_id' not in event:
        event['trace_id'] = 'aws-' + context.aws_request_id
    else:
        event['trace_id'] = 'aws-' + event['trace_id']
    sqs_message = dict()
    sqs_message["input"] = event
    sqs_message["name"] = context.function_name
    resultset_dict = dict()
    try:
        with conn.cursor() as cursor:
            params = (event['format'], event['company'], event['source'],
                      event['country'], event['queryDate'], event['yearMonth'],
                      event['accountId'], event['creditCard'], event['trace_id'])
            cursor.execute(sql, params)
            resultset_dict = ccb_sql.fetch_result(cursor)
    except pyodbc.Error as the_error:
        error_message = ""
        for elem in the_error.args:
            error_message = error_message + ' ' + elem
        sqs_message["error"] = error_message
        sqs_message["success"] = False
        sqs_message["level"] = 'ERROR'
    else:
        sqs_message["data"] = resultset_dict
        sqs_message["success"] = True
        sqs_message["level"] = 'INFO' if os.environ.get('TRACE_LEVEL') == None else os.environ.get('TRACE_LEVEL')
    sqs_message["date"] = datetime.datetime.now()
    sqs_message["duration"] = datetime.datetime.now() - start_clock
    sqs_conn.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/750819503324/ccb_services_logs',
        MessageBody=json.dumps(sqs_message, sort_keys=True, cls=ccb_global.DictionaryToJSON))
    result_str = json.dumps(resultset_dict, sort_keys=True, cls=ccb_global.DictionaryToJSON)
    if sqs_message["success"]:
        return {
            'success': True,
            'data': json.loads(result_str),
            'traceid': event['trace_id']
        }
    else:
        return {
            'success': False,
            'error': 'No fue posible completar la petición. La causa raíz se registró en las trazas del sistema.',
            'traceid': event['trace_id']
        }
