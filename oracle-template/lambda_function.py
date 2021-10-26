import json
import os
import datetime
import boto3
from ccb_toolbox import ccb_global
from ccb_toolbox import ccb_oracle


# Lambda Context:
# Obtener conexiones y Objetos usados en todas las invocaciones
# connection_data = ccb_global.get_secret("/dev/oracle/10g/prolinux")
connection_data = ccb_global.get_parameters(["/ha/oracle/19/panama-srv"])
my_conn = ccb_oracle.connect(connection_data)
collection_type = my_conn.gettype("OTT_CASHTRANSACTIONSLIST")
sqs_conn = boto3.client('sqs')


# Mock Handler
def mock_handler(event, context):
    return {
        "success": True,
        "message": "Información recuperada exitosamente.",
        "data": {
            "totalPages": 1,
            "totalRecords": 0,
            "responseCode": 200,
            "message": "OK",
            "pttCashtransactions": [
                {
                    "tmoCodusr": 5998,
                    "tmoNumtra": 91,
                    "tmoSec": 1,
                    "tmoCodtra": 1,
                    "tmoCodmod": 4,
                    "tmoCodpro": 2,
                    "tmoCodtip": 1,
                    "tmoCodmon": 0,
                    "tmoRubro": 1,
                    "tmoFechavig": "1980-01-01T00:00:00",
                    "tmoCodsuc": 1,
                    "tmoCodofi": 1,
                    "tmoFechor": "2021-08-12T10:56:53",
                    "tmoFechcon": "2021-08-12T00:00:00",
                    "tmoDepart": 20,
                    "tmoTabdep": 40,
                    "tmoModo": "N",
                    "tmoRef": "105642",
                    "tmoTipotra": "C",
                    "tmoVal": 1000,
                    "tmoTiporub": "E",
                    "tmoSuccue": 1,
                    "tmoOficue": 1,
                    "tmoNumcue": 4021751832,
                    "tmoEstcta": None,
                    "tmoTerminal": "192.168.112.178",
                    "tmoFechapos": None,
                    "tmoFecharev": None,
                    "tmoAutoriza": None,
                    "tmoCotiza": 1,
                    "tmoDesglo": "!",
                    "tmoFechproc": "2021-08-12T00:00:00",
                    "tmoDiferido": "N",
                    "tmoTipoid": "C",
                    "tmoNombre": None,
                    "tmoIdentifica": "8-722-123",
                    "empCargo": 36,
                    "empIdentifi": "8-722-169",
                    "empTipoid": "C",
                    "empNombre": "KATHYNIA MABEL RAMEA",
                    "card": None,
                    "uafTipoid": None,
                    "uafIdentifica": None,
                    "uafPrimernombre": None,
                    "uafSegundonombre": None,
                    "uafApepaterno": None,
                    "uafApematerno": None,
                    "uafTabtip": None,
                    "uafTipodir": None,
                    "uafTabpais": None,
                    "uafPais": None,
                    "uafTabprovincia": None,
                    "uafProvincia": None,
                    "uafTabdistrito": None,
                    "uafDistrito": None,
                    "uafTabciudad": None,
                    "uafCiudad": None,
                    "uafTabcorregimi": None,
                    "uafCorregimi": None,
                    "uafTabbarrio": None,
                    "uafBarrio": None,
                    "uafSector": None,
                    "uafTabcalle": None,
                    "uafTipocalle": None,
                    "uafCalle": None,
                    "uafNumcasa": None,
                    "uafEdificio": None,
                    "uafApartamento": None,
                    "uafPiso": None,
                    "uafDireccion": None,
                    "uafDireccion2": None,
                    "uafDireccion3": None,
                    "uafFechaingreso": None,
                    "uafCodusuarioin": None,
                    "uafFechacambio": None,
                    "uafCodusuariocam": None,
                    "uafTabNacDoc": None,
                    "uafNacDoc": None,
                    "mejModM": None,
                    "mejTraM": None,
                    "mejFlaghab": None,
                    "mejFlaghabcr": None,
                    "mejEfectivo": None,
                    "mejCuasiefe": None,
                    "mejValmmHab": None,
                    "mejOperacion": None,
                    "mejRubro": None,
                    "conLibre": None
                }
            ]
        },
        "error": None,
        "traceid": "mock-7491b17f-bd28-4301-a9a0-7ef4c2cddc7b"
    }


# Lambda Handler.
def lambda_handler(event, context):
    # Modo Mock:
    # Si la variable de ambiente MOCK_MODE se configura en TRUE la respuesta del servicio será tipo Mock
    mock_mode = False if os.environ.get('MOCK_MODE') == None or os.environ.get('MOCK_MODE') == 'FALSE' else True
    if mock_mode:
        return mock_handler(event, context)
    # Medir el tiempo de ejecución
    start_clock = datetime.datetime.now()
    # Obtener el ID de la invocación
    if 'trace_id' not in event:
        event['trace_id'] = 'aws-' + context.aws_request_id
    else:
        event['trace_id'] = 'aws-' + event['trace_id']
    # Ir preparando el mensaje para auditabilidad y monetización
    sqs_message = dict()
    sqs_message["input"] = event
    sqs_message["name"] = context.function_name
    response = []
    try:
        with my_conn.cursor() as cursor:
            request = {
                'P_FORMAT': event['format'],
                'P_COMPANY': event['company'],
                'P_SOURCE': event['source'],
                'P_COUNTRY': event['country'],
                'P_ACCOUNT_ID': event['accountId'],
                'P_DATE_FROM': datetime.datetime.fromisoformat(event['dateFrom']),
                'P_DATE_TO': datetime.datetime.fromisoformat(event['dateTo']),
                'P_TRACE_ID': event['trace_id'],
                'P_ITEMS_PER_PAGE': event['itemsPerPage'],
                'P_PAGE_NUMBER': event['pageNumber'],
                'P_TOTAL_PAGES': cursor.var(int), 'P_TOTAL_RECORDS': cursor.var(int),
                'P_RESPONSE_CODE': cursor.var(int), 'P_MESSAGE': cursor.var(str),
                'PTT_CASHTRANSACTIONS': cursor.var(collection_type)
            }
            parameters = ccb_oracle.declare_parameters(request)
            pl_sql = f"begin PKG_CCB_CORE_TRANSACTIONS.PRO_GET_CASH_TRANSACTIONS({parameters}); end;"
            cursor.execute(pl_sql, request)
            my_collection = request['PTT_CASHTRANSACTIONS'].getvalue().aslist()
            print(my_collection)
            response = ccb_oracle.fetch_collection(my_collection)
    except Exception as err:
        sqs_message["error"] = err.args[0].message
        sqs_message["success"] = False
        sqs_message["level"] = 'ERROR'
    else:
        sqs_message["data"] = response
        sqs_message["success"] = True
        sqs_message["level"] = 'INFO' if os.environ.get('TRACE_LEVEL') == None else os.environ.get('TRACE_LEVEL')
    sqs_message["date"] = datetime.datetime.now()
    sqs_message["duration"] = datetime.datetime.now() - start_clock
    sqs_conn.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/750819503324/ccb_services_logs',
        MessageBody=json.dumps(sqs_message, sort_keys=True, cls=ccb_global.DictionaryToJSON))
    result_str = json.dumps(response, sort_keys=True, cls=ccb_global.DictionaryToJSON)
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
