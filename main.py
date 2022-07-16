import json
import os
from query import Query
from chart import Chart
from helper import Helper
from database import Database

def get_chart_data(request):
     # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': "*",
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)
  
    headers = {
        'Access-Control-Allow-Origin': "*"
    }

    try:
        helper = Helper()
        database = Database()
        #transofmra el request a json
        request = request.get_json()

        sensors = database.get_sensors_data(
            sub_zone=request['sub_zone'],
            main_type=request['type'],
            variable=request['variable'],
            options=request['options']
        )

        #inicializa query de BIG QUERY
        query = Query();
        #Obtiene los resultados de la query
        rows = query.get_rows(helper.pluck(sensors,'id'),request['start_date'],request['end_date'])
        #convierte el resultado en un diccionario
        rows =[dict(row) for row in rows]
        if len(rows):
            #inicializa el objeto chart con las filas recibidas
            chart = Chart(
                sensors=sensors,
                rows=rows,
                start_date=request['start_date'],
                end_date=request['end_date'],
                main_type=request['type'],
                variable=request['variable'],
                opt=request['options']
            )
            #obtiene la data procesada en formato lista para highcharts
            chart.make_chart_data()
            database.close_connection()
            #imprime el resultado como json
            return json.dumps(chart.options), 200, headers
        else:
            #imprime json en blanco
            database.close_connection()
            return json.dumps([]), 200, headers
    except Exception as error:
        database.close_connection()
        return json.dumps(error), 422, headers
        raise    