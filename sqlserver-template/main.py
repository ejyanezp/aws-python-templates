# Use this file to implement Unit tests
import lambda_function

event = {
  "accountId": "0000004765340020328",
  "company": 1,
  "country": 1,
  "creditCard": "4765340008416734",
  "format": "JSON",
  "queryDate": "2021-05-24",
  "source": "CCB",
  "yearMonth": "2021-05",
  "sourceIP": "192.168.2.201"
}


class Context:
    aws_request_id = 'aws-sqltest-ey-1'
    function_name = 'arn:aws:lambda:us-east-1:750819503324:function:CCB_MSSQL_Python_Test'


context = Context()
response = lambda_function.lambda_handler(event, context)
print(response)
