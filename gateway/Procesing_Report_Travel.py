import warnings
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import time, datetime
import json
import pyodbc #Funcion necesaria para realizar la conexion con SQL Server
warnings.simplefilter('ignore')
import Patterns_travels
import Get_API_Data
import Alternative_function
import pytz

def Distance_Odometer_AVG(origin, destination, equipment, sql_connect):
  print('Se calcula la distancia promedio entre geocercas')
  with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as conexion:
    cursor = conexion.cursor()
    mainQueryDistance = """select TOP 10 eqc.equipment_name, am.name as origin, am2.name as destination, tr.time_stamp, tr.distance_meters 
                            from travels tr
                            INNER JOIN locations am ON am.location_id = tr.origin_location_id      
                            INNER JOIN locations am2 ON am2.location_id = tr.destination_location_id  
                            INNER JOIN equipment eqc ON tr.equipment_t_id=eqc.equipment_id
                            LEFT JOIN obs_travels obs ON tr.travel_id=obs.travel_id 
                            WHERE distance_meters is not null and am.name=? and am2.name=? AND distance_meters not in(0,9999999,999999,888888, -1)
                            AND eqc.equipment_name = ?
                            and obs.new_origin_id is null and obs.new_destination_id is null AND tr.distance_meters NOT IN (
                            SELECT MIN(distance_meters)
                            FROM travels
                            WHERE distance_meters IS NOT NULL
                                AND am.name = ?
                                AND am2.name = ?
                                AND eqc.equipment_name = ?
                                AND obs.new_origin_id IS NULL
                                AND obs.new_destination_id IS NULL
                            ) AND tr.distance_meters NOT IN (
                                SELECT MAX(distance_meters)
                                FROM travels
                                WHERE distance_meters IS NOT NULL
                                    AND am.name = ?
                                    AND am2.name = ?
                                    AND eqc.equipment_name = ?
                                    AND obs.new_origin_id IS NULL
                                    AND obs.new_destination_id IS NULL
                            )
                            ORDER BY tr.time_stamp desc
                        """
    secundaryQueryDistance = """select TOP 10 eqc.equipment_name, am.name as origin, am2.name as destination, tr.time_stamp, tr.distance_meters 
                                from travels tr
                                INNER JOIN locations am ON am.location_id = tr.origin_location_id      
                                INNER JOIN locations am2 ON am2.location_id = tr.destination_location_id  
                                INNER JOIN equipment eqc ON tr.equipment_t_id=eqc.equipment_id 
                                LEFT JOIN obs_travels obs ON tr.travel_id=obs.travel_id 
                                WHERE distance_meters is not null and am.name=? and am2.name=? AND distance_meters not in(0,9999999,999999,888888,-1)
                                AND tr.distance_meters NOT IN (
                                SELECT MIN(distance_meters)
                                FROM travels
                                WHERE distance_meters IS NOT NULL
                                    AND am.name = ?
                                    AND am2.name = ?
                                    AND obs.new_origin_id IS NULL
                                    AND obs.new_destination_id IS NULL
                                ) AND tr.distance_meters NOT IN (
                                    SELECT MAX(distance_meters)
                                    FROM travels
                                    WHERE distance_meters IS NOT NULL
                                        AND am.name = ?
                                        AND am2.name = ?
                                        AND obs.new_origin_id IS NULL
                                        AND obs.new_destination_id IS NULL
                                )
                                ORDER BY tr.time_stamp desc
                        """
    try:
        cursor.execute(mainQueryDistance, origin, destination, equipment, origin, destination, equipment, origin, destination, equipment)
        resultDistances = cursor.fetchall()
        if(len(resultDistances)>=5):
            totalData = len(resultDistances)
            avgDistance = 0
            for distance in resultDistances:
                avgDistance = avgDistance + distance[4]
            avgDistance = avgDistance/totalData
            return avgDistance
        else:
            print('El numero de resultados es muy bajo para el equipo')
            cursor.execute(secundaryQueryDistance, origin, destination, origin, destination, origin, destination)
            resultDistances = cursor.fetchall()
            if(len(resultDistances)==0):
                return 999999
            totalData = len(resultDistances)
            avgDistance = 0
            for distance in resultDistances:
                avgDistance = avgDistance + distance[4]
            avgDistance = avgDistance/totalData
            if (avgDistance<0):
              return 999999
            return avgDistance
    except Exception as err:
       print(err)
       return 999999

def New_Read_API_Reports(Begin_Request, End_Request, AppID, ReportID, EventRule, Tag, sql_connect, limit_time_stamp_shift_change, hour_production_definition, token_id):
  print('Entro al read')
  df_load_cycles      = pd.DataFrame(columns = ['Equipo','Cargador','Blast','Dump','time_depart', 'time_arrive', 'time_production_depart','time_production_arrive','time_spotting_and_spotted','time_production_spotting_and_spotted', 'time_stamp', 'time_production_stamp', 'traveling_time','inside_time'])
  df_load_cycles_post = pd.DataFrame(columns = ['Equipo','Cargador','Blast','Dump','time_depart', 'time_arrive', 'time_production_depart','time_production_arrive','time_spotting_and_spotted','time_production_spotting_and_spotted', 'time_stamp', 'time_production_stamp', 'traveling_time','inside_time'])
  flagMessageReport = False

  list_symbols = ['-',"\\",'|','/','-',"\\",'|','/']
  counter_output = 0
  result = None
  conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
  cursor = conexion.cursor()

  simberiTz = pytz.timezone('Australia/Brisbane')
  utcNow = datetime.datetime.utcnow()
  simberiNow = utcNow.replace(tzinfo=pytz.utc).astimezone(simberiTz)
  simberiFechaInicioReporte = simberiNow.strftime('%Y-%m-%d %H:%M:%S')
  while result is None:
    try:
      if counter_output%500 == 0:

        #Read_API_CustomFields(engine_string)
        print('Entra al ingrego posturas')
        #ingresoPosturas(sql_connect)
        #New_Read_API_CustomFields(sql_connect)
        #CAMBIAR ID APLICACION
        response_geogroups = requests.get("http://83.229.5.28:80/comGpsGate/api/v.1/applications/"+str(AppID)+"/geofencegroups", headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"})
        response_geogroups.status_code
        urlData = response_geogroups.content
        data_geogroups = json.loads(urlData)
        df_geogroups = pd.json_normalize( data_geogroups )
        df_geogroups = df_geogroups.rename(columns={"id": "id_group"})
        #CAMBIAR ID APLICACION
        response_render = requests.post("http://83.229.5.28:80/comGpsGate/api/v.1/applications/"+str(AppID)+"/reports/"+str(ReportID)+"/renderings",
                                  headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"}, 
                                  json={
          "parameters": [
            {
              "arrayValues": [],
              "parameterName": "Period",
              "periodStart": Begin_Request,
              "periodEnd": End_Request,
              "timeSpan": "",
              "value": "Custom", 
              "visible": 'true'
            },        
            {
                    "parameterName": "EventRule",
                    "visible": 'true',
                    "arrayValues": [
                        EventRule
                    ]
                },
                {
                    "parameterName": "Tag",
                    "visible": 'false',
                    "arrayValues": [
                        Tag
                    ]
                }

          ],
          "reportFormatId": 2, #1: HTML, 2: CSV, 3:PDF
          "reportId": ReportID,
          "sendEmail": 'false'})

        response_render.status_code
        urlData = response_render.content
        data_render = json.loads(urlData)
        print(data_render)
      #CAMBIAR ID APLICACION
      time.sleep(0.5)
      response_output = requests.get("http://83.229.5.28:80/comGpsGate/api/v.1/applications/"+str(AppID)+"/reports/"+str(ReportID)+"/renderings/"+str(data_render['id']), headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"})

      
      response_output.status_code
      urlData = response_output.content
      data_output = json.loads(urlData)

      url = "http://83.229.5.28:80" + str(data_output['outputFile'])
      #CAMBIAR ID APLICACION
      headers = {'Authorization': 'OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB'}
      result = 1

    except:
      counter_output += 1
      #Si el reporte se demora mas de 30 minutos 
      if(counter_output == 7200 and flagMessageReport==False):
        utcNow = datetime.datetime.utcnow()
        simberiNowError = utcNow.replace(tzinfo=pytz.utc).astimezone(simberiTz)
        simberiFechaError= simberiNowError.strftime('%Y-%m-%d %H:%M:%S')
        print('El sistema esta intentando solicitar un reporte desde las ... horas')
        messageErrorReports = 'El gateway lleva mas de una hora intentando recolectar los reportes. Hora inicial: ' + simberiFechaInicioReporte + ' Hora actual: ' + simberiFechaError
        Alternative_function.telegramBot(messageErrorReports)
        flagMessageReport = True
      print("Tryng to get Output File", list_symbols[counter_output%8], counter_output, end='\r', flush=True)
      pass
  print('\n')
  print('Paso esta etapa')
  bytes_data = requests.get(url, headers=headers).content
  s=str(bytes_data,'utf-8')
  data = StringIO(s) 
  df_report=pd.read_csv(data, sep=",")
  #print( "----------------- Número de registros: " ,len(df_report))

  try:
    end_date = datetime.date.today() + datetime.timedelta(days=1)
    start_date = end_date + datetime.timedelta(days=-2)
    fecha_events = str(start_date.strftime("%Y-%m-%d"))
    #Get_API_Data.get_beacon_information(start_date, end_date, sql_connect)
    Get_API_Data.main_read_events(fecha_events, end_date,False, sql_connect, AppID, Tag, token_id)
  except Exception as err:
    print('Error al recolectar informacion')
    fecha_hora_actual = datetime.datetime.now()
    messageRecolectionGTS = 'Fallo el proceso de recoleccion GTS: ' + str(fecha_hora_actual)
    Alternative_function.create_log_file('log_reconciler_distance.log', messageRecolectionGTS)
    print(err)

  print(df_report)
  df_report['Arrived Time'] = pd.to_datetime(df_report['Arrived Time'])
  df_report['Departed Time'] = pd.to_datetime(df_report['Departed Time'], errors='coerce')
  df_report = df_report[df_report['Departed Time'].notna()]
  df_report = df_report[df_report['Equipment'].notna()]
  df_report['Traveling Time'] = ""
  df_report['Groups'] = ""

  df_report_pro = pd.DataFrame()
  print('paso esta etapa 2')

  for j in df_report.Equipment.unique(): #Analize each truck one by one
    df_report_1 = df_report[ df_report['Equipment'] == j ].reset_index().drop('index', axis=1)
    for i in range(len(df_report_1)):
      
      try: 
        x = time.strptime( df_report_1.Duration[i].split(',')[0],'%H:%M:%S' )
        df_report_1.Duration[i] = datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
      except:
        try:
          x = time.strptime( df_report_1.Duration[i].split(',')[0],'%d:%H:%M:%S' )
          df_report_1.Duration[i] = datetime.timedelta(days=x.tm_mday,hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
        except:
          x = time.strptime( df_report_1.Duration[i].split(',')[0],'%m:%d:%H:%M:%S' )
          df_report_1.Duration[i] = datetime.timedelta(months=x.tm_mon, days=x.tm_mday,hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()

      if pd.isnull(df_report_1["Departed Time"][i]):
        #print(df_report_1['Duration'][i])
        df_report_1["Departed Time"][i] = df_report_1["Arrived Time"][i] + timedelta( seconds = df_report_1['Duration'][i] )
      if i > 0:
        travel_time = df_report_1['Arrived Time'][i] - df_report_1['Departed Time'][i-1] 
        x = travel_time.total_seconds()
        try: 
          df_report_1['Traveling Time'][i] = int(x)
        except:
          df_report_1['Traveling Time'][i] = 0

      df_report_1.Groups[i] = df_report_1.Category[i]

    df_report_pro = df_report_pro.append(df_report_1)
  df_report_pro = df_report_pro.reset_index().drop('index', axis=1)

  def diff_dates(date1, date2):
      return abs(date2-date1).days

  print('\n')
  counter_elements = 0

  #--------------------------INICIO Preprocesamiento 2 --------------------------
  query_get_stopped = 'SELECT * FROM event_stop_in_geofence where truck_name=? AND time_start >=?  AND time_end<=?;'
  for j in df_report.Equipment.unique():
    print('Error en recoleccion')
    print(j)
    fecha_ultimo_viaje = None
    try:
        query_select_last_travel = '''SELECT TOP 1 tv.time_stamp FROM travels tv, equipment eq
                                        WHERE tv.equipment_t_id=eq.equipment_id AND equipment_name=? order by tv.time_stamp desc;
                                    '''
        cursor.execute(query_select_last_travel, str(int(j)))
        fecha_ultimo_viaje = cursor.fetchone()[0]
    except Exception as err:
        print(err)
        fecha_ultimo_viaje = datetime.datetime.fromisoformat('2022-12-01 00:00:00.000')
    print('EQUIPO ' + str(j))
    print('FECHA ULTIMO registro ' + str(fecha_ultimo_viaje))
    
    df_report_1 = df_report_pro[ df_report_pro['Equipment'] == j ].reset_index().drop('index', axis=1)
    print('Entra al if')
    for i in range(len(df_report_1)):
      counter_elements += 1
      if i > 0:   
        #DEBO VALIDAR LA HORA MAXIMA
        print('------------------------------------------')
        print('La fecha de evaluacion para LOS VIAJES ES')
        print(limit_time_stamp_shift_change)
        print('La fecha del ultimo viaje registrado es')
        print(fecha_ultimo_viaje)
        print('La fecha del nuevo viaje es')
        print(df_report_1['Departed Time'][i])
        
        print('------------------------------------------')
        if(df_report_1['Departed Time'][i]>fecha_ultimo_viaje and df_report_1['Departed Time'][i]>=limit_time_stamp_shift_change): 
          print('cumple condicion: fecha corte')
          travel_time = None
          duration_origin = 0
          duration_destination = 0
          start_stop_origin = None
          start_stop_destination = None
          end_stop_origin = None
          end_stop_destination = None
          traveling_time = 0
          case = 1
          main_end_stop_origin = None
          main_start_stop_destination = None
          travelDistance = None
          odometer_start_stop_origin = None
          odometer_end_stop_origin = None
          odometer_start_stop_destination = None
          odometer_main_origin = None
          odometer_main_destination = None
          odometer_end_stop_destination = None
          #AQUI SE EJECUTA LA QUERY PARA SABER SI EL EQUIPO TUVO ALGUN EVENTO DE DETENCION
          try:
            cursor.execute(query_get_stopped,str(int(j)),df_report_1['Arrived Time'][i-1], df_report_1['Departed Time'][i-1])
            result_origin = cursor.fetchall()
            if(len(result_origin)>0):
              print(result_origin)
              start_stop_origin = result_origin[0][4]
              end_stop_origin = result_origin[-1][5]              
              odometer_start_stop_origin = result_origin[0][7]
              odometer_end_stop_origin = result_origin[-1][7]
              for row in result_origin:
                duration_origin = duration_origin + row[6]
            cursor.execute(query_get_stopped,str(int(j)),df_report_1['Arrived Time'][i], df_report_1['Departed Time'][i])
            result_destination = cursor.fetchall()
            if(len(result_destination)>0):
              start_stop_destination = result_destination[0][4]
              end_stop_destination = result_destination[-1][5]
              
              odometer_start_stop_destination = result_destination[0][7]
              odometer_end_stop_destination = result_destination[-1][7]
              for row in result_destination:
                duration_destination = duration_destination + row[6]
          except Exception as err:
            print(err)
                  #MANEJO DE CASOS
          #CASO 1: ORIGEN Y DESTINO NO TIENEN EVENTOS DE STOP
          if(duration_origin==0 and duration_destination==0):
            print('case 1')
            travel_time = df_report_1['Arrived Time'][i] - df_report_1['Departed Time'][i-1]
            case = 1
            main_end_stop_origin = df_report_1['Departed Time'][i-1]
            main_start_stop_destination = df_report_1['Arrived Time'][i]
            #CASO 2: ORIGEN Y DESTINO TIENEN EVENTOS STOP
          elif(duration_origin>0 and duration_destination>0):
            print('case 2')
            travel_time = start_stop_destination - end_stop_origin
            case = 2
            main_end_stop_origin = end_stop_origin
            main_start_stop_destination = start_stop_destination
            #CASO 3: ORIGEN NO TIENE EVENTOS Y DESTINO SI
          elif(duration_origin==0 and duration_destination>0):
            print('case 3')
            travel_time = start_stop_destination - df_report_1['Departed Time'][i-1]
            case = 3
            main_end_stop_origin =  df_report_1['Departed Time'][i-1]
            main_start_stop_destination = start_stop_destination
            #CASO 4: ORIGEN TIENE EVENTOS PERO DESTINO NO
          elif(duration_origin>0 and duration_destination==0):
            print('case 4')
            travel_time = df_report_1['Arrived Time'][i] - end_stop_origin
            case = 4
            main_end_stop_origin = end_stop_origin
            main_start_stop_destination =  df_report_1['Arrived Time'][i]
          else:
            print('Este caso no deberia existir')

          
          #--------Calculo Distancia Odometro -------------.
          if(case==2):
            print('Esto es lo optimo')
            try:
              travelDistance = odometer_start_stop_destination - odometer_end_stop_origin
            except:
              travelDistance = Distance_Odometer_AVG(df_report_1.Geofence_Name[i-1],
                                                     df_report_1.Geofence_Name[i],
                                                     df_report_1.Equipment[i],
                                                     sql_connect)
          else:
            print('Se debe calcular un promedio')
            travelDistance = Distance_Odometer_AVG(df_report_1.Geofence_Name[i-1],
                                                   df_report_1.Geofence_Name[i],
                                                   df_report_1.Equipment[i], 
                                                   sql_connect)
          odometer_main_origin = odometer_end_stop_origin
          odometer_main_destination = odometer_start_stop_destination


          #print(counter_elements, ' Travel Registers Complete', end='\r', flush=True)
          #------------------------------ Calculo de Tiempo de Salida en base a duración en geofence -------------------------------------
          time_spotting_and_spotted = df_report_1['Arrived Time'][i] + datetime.timedelta(seconds=300)
          if (time_spotting_and_spotted > df_report_1['Departed Time'][i]):
            time_spotting_and_spotted = df_report_1['Arrived Time'][i]
          #------------------------------ Definicion de Turno Día/Noche ------------------------------------------------------------------
          shift = 'Night'
          if ( int(df_report_1['Departed Time'][i].strftime("%H")) >= 6 and int(df_report_1['Departed Time'][i].strftime("%H")) < 18):
            shift = 'Day'
          #------------------------------ Definición de Cuadrilla ------------------------------------------------------------------------
          production_date = df_report_1['Departed Time'][i] - datetime.timedelta(hours=8)


          query_crew = '''select CASE 
                        when DATEPART(HOUR, ?)>=6 and DATEPART(HOUR, ?)<18 THEN 'Day'
                        ELSE 'Night'
                       END AS shift,
                       DATEDIFF(day,'2024-07-17 00:00:00.000', dateadd(HOUR, -6, ?)) % 28 AS cycle_stage
                        '''
          cursor.execute(query_crew, df_report_1['Departed Time'][i], df_report_1['Departed Time'][i],df_report_1['Departed Time'][i])
          result_crew = cursor.fetchone()
          crew = None
          if (int(result_crew[1]) >= 0 and int(result_crew[1]) <= 6 and result_crew[0] == 'Day'):
              crew = 2
          elif (int(result_crew[1]) >= 0 and int(result_crew[1]) <= 6 and result_crew[0] == 'Night'):
              crew = 6
          elif (int(result_crew[1]) >= 7 and int(result_crew[1]) <= 13 and result_crew[0] == 'Day'):
              crew = 1
          elif (int(result_crew[1]) >= 7 and int(result_crew[1]) <= 13 and result_crew[0] == 'Night'):
              crew = 3
          elif (int(result_crew[1]) >= 14 and int(result_crew[1]) <= 20 and result_crew[0] == 'Day'):
              crew = 6
          elif (int(result_crew[1]) >= 14 and int(result_crew[1]) <= 20 and result_crew[0] == 'Night'):
              crew = 2
          elif (int(result_crew[1]) >= 21 and int(result_crew[1]) <= 27 and result_crew[0] == 'Day'):
              crew = 3
          elif (int(result_crew[1]) >= 21 and int(result_crew[1]) <= 27 and result_crew[0] == 'Night'):
              crew = 1

          #------------------------------ Definición tipo de Trayecto -------------------------------------------------
          same_type_travel = 0
          if df_report_1.Groups[i] == df_report_1.Groups[i-1]:
            same_type_travel = 1

          # otro=4 cargado=1 vacio=2, a observar=3
          #AQUI SE CAMBIA PARA SIMBERI --SE AGREGA INPIT AL LA LOGICA
          tipo_trayecto = 4
          if   ((df_report_1.Groups[i-1] == 'Blasts' and df_report_1.Groups[i] == 'Dumps')  or
              (df_report_1.Groups[i-1] == 'Stockpiles' and df_report_1.Groups[i] == 'Dumps') or
              (df_report_1.Groups[i-1] == 'Blast' and df_report_1.Groups[i] == 'Inpits') or
              (df_report_1.Groups[i-1] == 'Stockpiles' and df_report_1.Groups[i] == 'Inpits') or
              (df_report_1.Groups[i-1] == 'Blasts' and df_report_1.Groups[i] == 'Stockpiles') or
              (df_report_1.Groups[i-1] == 'Blasts' and df_report_1.Groups[i] == 'Crushers') or
              (df_report_1.Groups[i-1] == 'Stockpiles' and df_report_1.Groups[i] == 'Crushers')):
            tipo_trayecto = 1
          elif ((df_report_1.Groups[i-1] == 'Dumps' and df_report_1.Groups[i] == 'Blasts')  or
              (df_report_1.Groups[i-1] == 'Dumps' and df_report_1.Groups[i] == 'Stockpiles') or
              (df_report_1.Groups[i-1] == 'Inpits' and df_report_1.Groups[i] == 'Blasts') or
              (df_report_1.Groups[i-1] == 'Inpits' and df_report_1.Groups[i] == 'Stockpiles') or
              (df_report_1.Groups[i-1] == 'Inpits' and df_report_1.Groups[i] == 'Inpits') or
              (df_report_1.Groups[i-1] == 'Stockpiles' and df_report_1.Groups[i] == 'Blasts') or
              (df_report_1.Groups[i-1] == 'Crushers' and df_report_1.Groups[i] == 'Blasts') or
              (df_report_1.Groups[i-1] == 'Crushers' and df_report_1.Groups[i] == 'Stockpiles')):
            tipo_trayecto = 2
          elif ((df_report_1.Groups[i-1] == 'Dumps' and df_report_1.Groups[i] == 'Dumps') or
              (df_report_1.Groups[i-1] == 'Blasts' and df_report_1.Groups[i] == 'Blasts') or
              (df_report_1.Groups[i-1] == 'Stockpiles' and df_report_1.Groups[i] == 'Stockpiles') or
              (df_report_1.Groups[i-1] == 'Crushers' and df_report_1.Groups[i] == 'Dumps') or
              (df_report_1.Groups[i-1] == 'Dumps' and df_report_1.Groups[i] == 'Crushers') or
              (df_report_1.Groups[i-1] == 'Crushers' and df_report_1.Groups[i] == 'Crushers')):
            tipo_trayecto = 3



          
          #------------------------------------- Adjuntar Registro en Dataframe ------------------------------------------------
          if(df_report_1['Departed Time'][i]>fecha_ultimo_viaje):
              print('Cumple la condicion ' + str(df_report_1['Departed Time'][i]))
              df_load_cycles = df_load_cycles.append({  'Equipo_id':df_report_1.Equipment_ID[i],
                                                        'Equipo':df_report_1.Equipment[i],
                                                        'Blast_id':df_report_1.Geofence_ID[i-1],
                                                        'Blast':df_report_1.Geofence_Name[i-1],
                                                        'Dump_id':df_report_1.Geofence_ID[i],
                                                        'Dump':df_report_1.Geofence_Name[i],
                                                        'Categoria_origen':df_report_1.Groups[i-1],
                                                        'Categoria_destino':df_report_1.Groups[i],
                                                        'Tipo_trayecto': tipo_trayecto,
                                                        'Tonelaje':df_report_1.Tonelaje[i],
                                                        'time_depart':df_report_1['Departed Time'][i-1],
                                                        'time_production_depart':df_report_1['Departed Time'][i-1] - datetime.timedelta(hours=hour_production_definition),
                                                        'time_arrive':df_report_1['Arrived Time'][i], 
                                                        'time_production_arrive':df_report_1['Arrived Time'][i] - datetime.timedelta(hours=hour_production_definition),
                                                        'time_spotting_and_spotted':time_spotting_and_spotted,
                                                        'time_production_spotting_and_spotted':time_spotting_and_spotted - datetime.timedelta(hours=hour_production_definition),
                                                        'time_stamp':df_report_1['Departed Time'][i],
                                                        'time_production_stamp':df_report_1['Departed Time'][i] - datetime.timedelta(hours=hour_production_definition),
                                                        'traveling_time': int(travel_time.total_seconds())/60,
                                                        'inside_time':df_report_1['Duration'][i]/60,
                                                        'Turno':shift,
                                                        'Cuadrilla':crew,
                                                        'index':str(df_report_1['Departed Time'][i].strftime('%y%m%d%H%M%S')) + str(int(df_report_1.Equipment_ID[i])),
                                                        'travel_same_type':same_type_travel,
                                                        'time_origin_start':df_report_1['Arrived Time'][i-1],
                                                        'inside_time_origin':df_report_1['Duration'][i-1]/60,
                                                        'origin_end_stop': main_end_stop_origin,
                                                        'destination_start_stop': main_start_stop_destination,
                                                        'case': case,
                                                        'travel_distance': travelDistance,
                                                        'odometer_origin': odometer_main_origin,
                                                        'odometer_destination': odometer_main_destination 
                                                        }, ignore_index=True)

  print('El total de datos es ' + str(df_load_cycles.shape[0]))
  df_load_cycles = df_load_cycles.set_index('index')
  df_load_cycles = df_load_cycles.sort_values('time_stamp')
  
  print('\n')
  print(df_load_cycles)
  
  #------------------------------------------- PostProcesamiento ---------------------------------------
  print('\nExecuting Post Processing Algorithm')
  df_load_cycles_post = Patterns_travels.patron_blast_blast_dump(df_load_cycles)
  print('\n')
  print('Termin el patron_blast_blast_dump')
  df_load_cycles_post = Patterns_travels.patron_385(df_load_cycles_post)

  #first_message = 'Prueba1: El tamaño del reporte es: ' + str(tamano_antes_patron[0]) + ' - ' + str(tamano_antes_patron[1])
  #print(first_message)
  #df_load_cycles_post.to_csv('antes_de_patron.csv', index=True)
  #Alternative_function.telegramBot(first_message)
  df_load_cycles_post = Patterns_travels.PatternBlastRoardSheetingDump(df_load_cycles_post, sql_connect)
  #tamano_despues_patron = df_load_cycles_post.shape
  #last_message = 'Prueba2: El tamaño del reporte es: ' + str(tamano_despues_patron[0]) + ' - ' + str(tamano_despues_patron[1])
  #print(last_message)
  #df_load_cycles_post.to_csv('despues_de_patron.csv', index=True)
  #Alternative_function.telegramBot(last_message)


  df_load_cycles_post = df_load_cycles_post.set_index('index')
  df_load_cycles_post = df_load_cycles_post.sort_values('time_stamp')
  #------------------------------------------- END POSTPROCESAMIENTO ---------------------------------------
  print('\n')
  print(df_load_cycles_post)

  #------------------------------------------- Asignacion Beacon ---------------------------------------
  

  #------------------------------------------- Asignacion Beacon ---------------------------------------
  print('\nN normal cycles:', len(df_load_cycles))
  print('N postprocessed cycles:', len(df_load_cycles_post))
  #engine = create_engine('postgresql://postgres:fu6fo81me@localhost/Mining_bpv')
  #df_load_cycles.to_sql("raw_registro_viajes_2", engine, if_exists='replace')
  return df_load_cycles_post
