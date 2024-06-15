# Group 11
# Ziqiang Li, 1173898
# Donghao Yang, 1514687
# Rui Mao, 1469805
# Xiaxuan Du, 1481272
# Ruoyu Lu, 1466195

import logging, json
from flask import current_app, request
from elasticsearch8 import Elasticsearch
from string import Template

days_expr = Template('''{
        "bool": {
            "must": [
                {
                    "match": {
                        "country": "${country}"
                    }
                }
            ]
        }
}''')

aggregations = Template('''{
    "sum_sentiment": {
        "sum": {
            "field": "${field}"
        }
    }
}''')

def main():
    country = request.headers.get('X-Fission-Params-Country', 'au')
    field = request.headers.get('X-Fission-Params-Field', 'sentimentcount')
    current_app.logger.info(f'Received request with country: {country}')
    current_app.logger.info(f'Received request with field: {field}')

    client = Elasticsearch(
        'https://elasticsearch-master.elastic.svc.cluster.local:9200',
        verify_certs=False,
        ssl_show_warn=False,
        basic_auth=('elastic', 'elastic')
    )
    expr = days_expr.substitute(country=country)
    agg = aggregations.substitute(field=field)

    res = client.search(
        index='sentiments*',
        body={
            "query": json.loads(expr),
            "aggregations": json.loads(agg)
        }
    )

    return json.dumps(res['aggregations'])
