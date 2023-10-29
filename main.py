import requests
import pickle
import numpy as np
from google.cloud import storage
from flask import jsonify
import pandas as pd
import pyodbc
import os 
from google.cloud.sql.connector import Connector, IPTypes
import pytds
import sqlalchemy

def connect_with_connector() -> sqlalchemy.engine.base.Engine:

    db_user = "sqlserver"
    db_pass = "paladarpets123"
    db_name = "prjPets"  
    instance_connection_name = "deductive-notch-396522:southamerica-east1:paladarpets"  
  
    ip_type = IPTypes.PRIVATE if "10.3.160.4" else IPTypes.PUBLIC

    connector = Connector(ip_type)

    def getconn() -> pytds.Connection:
        conn = connector.connect(
            instance_connection_name,
            "pytds",
            user=db_user,
            password=db_pass,
            db=db_name
        )
        return conn

    pool = sqlalchemy.create_engine(
        "mssql+pytds://",
        creator=getconn,
    )

    return pool

def switch_case(new_ration_pred_class):
    switch_dict = {
        0: 'A',
        1: 'B',
        2: 'C',
    }

    return switch_dict.get(new_ration_pred_class, 'Outro valor padrão')

def classifier(request):
    if request.method == 'GET':

        codigoBarra = request.args.get('codigoBarra')

        storage_client = storage.Client()
        bucket = storage_client.get_bucket('test_model_buckets')  
        
        blob = bucket.blob('models/model_pets3.pkl')
        blob.download_to_filename('/tmp/model.pkl')
        model = pickle.load(open('/tmp/model.pkl', 'rb'))

        db_engine = connect_with_connector()

        sql_query = f"""
                    SELECT 
                        porcentagemProteinaBrutaMin,
                        umidadeMaxGKG,
                        porcentagemProteinaBrutaMin,
                        proteinaBrutaMinGKG,
                        proteinaMateriaSeca,
                        porcentagemCalcioMax,
                        calcioMaxGKG,
                        calcioMateriaSeca,
                        porcentagemMateriaFibrosa,
                        materiaFibrosaGKG
                    FROM [dbo].[GarantiaProduto]
                    where codigoBarra = {codigoBarra}
                    """

        query = sqlalchemy.text(sql_query)

        with db_engine.connect() as conn:
            result = conn.execute(query)
            first_row = result.first()
            conn.close()

       
        if first_row is not None:
            # Converter a tupla em um dicionário
            column_names = result.keys()
            row_data = dict(zip(column_names, first_row))

            dados_np = np.array([float(value) for value in first_row])

            dados_np = np.array(dados_np).reshape(1, -1)

            new_ration_pred_probs = model.predict(dados_np)

            new_ration_pred_class = np.argmax(new_ration_pred_probs, axis=1)[0]

            classPet = switch_case(int(new_ration_pred_class))

            response = {
                'cluster': int(new_ration_pred_class),
                'class': classPet,
                'sql_data': row_data
            }
        else:
            response = {
                'error': 'Nenhum resultado encontrado'
            }

        resHeader = jsonify(response)

        resHeader.headers.add('Access-Control-Allow-Origin', '*')
        resHeader.headers.add('Access-Control-Allow-Methods', 'GET')
        resHeader.headers.add('Access-Control-Allow-Headers', 'Content-Type')

        return resHeader