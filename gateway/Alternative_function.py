import warnings
import requests
import logging
import time 
import json
import pyodbc
warnings.simplefilter('ignore')

def telegramBot(mensajeEnviar):
    bot_token = '1953947198:AAEtKC4dFyanjny8qH-6ByGdRKuBuDZ7D30'
    bot_chatID = '-1001374282525'

    mensaje = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + mensajeEnviar

    requests.get(mensaje)

def create_log_file(file_name, message):
    try:
        with open(file_name, 'a') as file:
            file.write(message + '\n')
    except:
        pass

def cleanProductionTravelsSummary(sqlConnect):
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        queryGetDateDelete = "SELECT DATEADD(d,-3,MAX(date_shift)) from travels_latest_records"
        queryDeleteProduction = "DELETE from travels_latest_records where date_shift<=?;"
        queryDeleteExtraload = "DELETE from travels_extraload_latest_records where date_shift<=?;"
        try:
            cursor.execute(queryGetDateDelete)
            dateDelete = cursor.fetchone()[0]
            print(dateDelete)
            cursor.execute(queryDeleteProduction, dateDelete)
            cursor.execute(queryDeleteExtraload, dateDelete)
        except Exception as err:
            print(err)

def upload_shovel_status(id, json_data):
    try:
        url = "https://simplefms.io/comGpsGate/api/v.1/applications/13/users/"+str(id)+"/customfields/Time Category Shovel"
        headers = {
            'Authorization': 'OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB',
            'Content-Type': 'application/json'
        }
        payload = {
            'name': 'Time Category Shovel',
            'value': json_data
        }
        response = requests.put(url, headers=headers, data=json.dumps(payload))
        #print('Código de estado:', response.status_code)
        #print('Contenido de la respuesta:', response.text)
    except Exception as err:
        print(err)

def main_upload_shovel_status(sql_connect):
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
    cursor = conexion.cursor()
    array_shovel = []
    json_data = ''
    try: 
        query_get_status_shovel = """SELECT * FROM view_hist_states where equipment_type_name='Excavator' and state='active' and time_category_name='Operating'"""
        cursor.execute(query_get_status_shovel)
        operating_shovel = cursor.fetchall()
        cursor.close()
        
        response_trucks = requests.get("http://83.229.5.28/comGpsGate/api/v.1/applications/13/tags/153/users", headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"})
        response_trucks.status_code
        urlData = response_trucks.content
        data_trucks = json.loads(urlData)

        for shovel in operating_shovel:
            aux_data = {'Shovel':shovel[2]}
            array_shovel.append(aux_data)
        if(len(array_shovel)>0):
            json_data = json.dumps(array_shovel)
        
        for i in data_trucks:
            aux_camion = {'id':i['id'], 'name':i['name']}
            #print(aux_camion)
            upload_shovel_status(i['id'],json_data)
    except Exception as err:
        cursor.close()