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
                        "${field}": "${para}"
                    }
                }
            ]
        }
}''')

aggregations = Template('''{
    "sum_crash": {
        "sum": {
            "field": "_version"
        }
    }
}''')


def getcrashcount(expr):
    aggs = aggregations.substitute()
    client = Elasticsearch(
        'https://elasticsearch-master.elastic.svc.cluster.local:9200',
        verify_certs=False,
        ssl_show_warn=False,
        basic_auth=('elastic', 'elastic')
    )
    res = client.search(
        index='crash*',
        body={
            "query": json.loads(expr),
            "aggregations": json.loads(aggs)
        }
    )
    return json.dumps(res['aggregations']['sum_crash']['value'])


def main():
    field = request.headers.get('X-Fission-Params-Field', 'speed_zone')
    para = request.headers.get('X-Fission-Params-Para', 'all')
    current_app.logger.info(f'Received request with field: {field}')
    current_app.logger.info(f'Received request with para: {para}')

    result = {}
    if field == 'speed_zone' and para == 'all':
        for speed_value in ['<40', '040', '050', '060', '070', '080', '090', '100', '110', 'Not Known']:
            expr = days_expr.substitute(para=speed_value, field=field)
            result[speed_value] = getcrashcount(expr)
    elif field == 'light_condition' and para == 'all':
        for condition in ['Daylight', 'Darkness (with street light)', 'Darkness (without street light)',
                          'Dawn / Dusk', 'Not known']:
            expr = days_expr.substitute(para=condition, field=field)
            result[condition] = getcrashcount(expr)
    elif field == 'severity' and para == 'all':
        for condition in ['Property Damage Only', 'First Aid', 'Minor', 'Serious', 'Fatal']:
            expr = days_expr.substitute(para=condition, field=field)
            result[condition] = getcrashcount(expr)
    else:
        expr = days_expr.substitute(para=para, field=field)
        result = getcrashcount(expr)
    return result
