# Use this file to implement Unit tests
import lambda_function


event = {
  "accountId": 4030142848,
  "company": 1,
  "country": 1,
  "dateFrom": "2018-12-01",
  "dateTo": "2019-01-03",
  "format": "JSON",
  "itemsPerPage": 10,
  "pageNumber": 1,
  "source": "ATOMOS"
}


class Context:
    aws_request_id = 'aws-test-ey'
    function_name = 'arn:aws:lambda:us-east-1:750819503324:function:CCB_Oracle_Python_Test'


context = Context()
response = lambda_function.lambda_handler(event, context)
print(response)

