import requests
import pandas as pd
from datetime import datetime, timedelta
import datetime
import json
import sys, os
import Get_Data_BD
import pyodbc #Funcion necesaria para realizar la conexion con SQL Server

def llenado_Equipos(totalEquipos, tipoEquipo, tipoFlota, sql_connect):
    contadorInsert = 0
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
    cursor = conexion.cursor()
    for x in totalEquipos:
        try:
            #CHANGE QUERY LISTO?
            queryInsert = 'insert into equipment values (?,?,?,?,?,?,?)'
            cursor.execute(queryInsert, 
                        x['id'], 1, tipoEquipo, tipoFlota, x['nombre'],None,1
                        )
            conexion.commit()
            contadorInsert += 1
        except Exception as ex:
            pass
    return contadorInsert

def llamado_Equipos(sql_connect):
    contador_Equipos = 0
    #CAMBIAR ID APLICACION LISTO?
    respuestaServer = requests.head("https://simplefms.io/comGpsGate/api/v.1//applications/13/tags", 
                        headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"}
                        )
    if(respuestaServer.status_code==200):
      #CAMBIAR ID APLICACION LISTO?
        response_Tags= requests.get("https://simplefms.io/comGpsGate/api/v.1//applications/13/tags", 
                        headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"}
                        )
        urlData = response_Tags.content
        data_customfield = json.loads(urlData)
        listadoPalas = [x for x in data_customfield if x['name'] == 'Shovel']
        listadoCamiones = [x for x in data_customfield if x['name'] == 'Truck']
        listadoCamiones2 = [x for x in data_customfield if x['name'] == 'Camión']

        listadoPalasOk = []
        listadoCamionesOk = []
        #CAMBIAR ID APLICACION LISTO?
        for x in listadoPalas[0]['usersIds']:
            response_User= requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/13/users/" + str(x), 
                        headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"}
                        )
            urlData = response_User.content
            data_User = json.loads(urlData)
            auxPalas = {'id':data_User['id'], 'nombre': data_User['name']}
            listadoPalasOk.append(auxPalas)
        for x in listadoCamiones[0]['usersIds']:
          #CAMBIAR ID APLICACION LISTO?
            response_User= requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/13/users/" + str(x), 
                                headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"}
                                )
            urlData = response_User.content
            data_User = json.loads(urlData)
            auxXCamion = {'id':data_User['id'], 'nombre': data_User['name']}
            listadoCamionesOk.append(auxXCamion)
        for x in listadoCamiones2[0]['usersIds']:
          #CAMBIAR ID APLICACION LISTO?
            response_User= requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/13/users/" + str(x), 
                                headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"}
                                )
            urlData = response_User.content
            data_User = json.loads(urlData)
            auxXCamion = {'id':data_User['id'], 'nombre': data_User['name']}
            listadoCamionesOk.append(auxXCamion)
        aux1 = llenado_Equipos(listadoPalasOk,78,2,sql_connect)
        contador_Equipos = contador_Equipos + aux1
        aux2 = llenado_Equipos(listadoCamionesOk,79,1,sql_connect)
        contador_Equipos = contador_Equipos + aux2

        if(contador_Equipos == 0):
          print('No se agregaron nuevos equipos')
        else:
          print('Equipos nuevos = ' + str(contador_Equipos))


    contadorZonas = 0
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
    cursor = conexion.cursor()
    listado_Tipos_Zona = []
    #CAMBIAR ID APLICACION
    response_Tipos_Zona= requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/13/geofencegroups", 
                        headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"}
                        )
    urlData = response_Tipos_Zona.content

    data_Tipos_Zona = json.loads(urlData)
    for x in data_Tipos_Zona :
        auxTipoZona = {'id':x['id'], 'nombre':x['name'], 'descripcion':x['description'], 'zonas':x['geofenceIds']}
        listado_Tipos_Zona.append(auxTipoZona)
    
    for x in listado_Tipos_Zona:
        for i in x['zonas']:
          #CAMBIAR ID APLICACION
            response_Zonas = requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/7/geofences/"+ str(i), 
                    headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"}
                    )
            urlData = response_Zonas.content
            data_Zona = json.loads(urlData)
            try:
                #CHANGE QUERY LISTO?
                query = 'insert into locations values (?,?,?,?,?,?,?,?,?,?)'
                cursor.execute(query, 1, x['id'],3, data_Zona['id'],data_Zona['name'],None, None, None,None,None)
                conexion.commit()
                print('area insertada')
                contadorZonas +=1
            except Exception as ex:
                #print(ex)
                pass
    if(contadorZonas==0):
      print('No se agregaron nuevas zonas en la mina')
    else:
      print('Se agregaron ' +str(contadorZonas)+ ' nuevas zonas mineras')

def cargarZonas(sql_connect, aplication_id_gps, token_id_gps):
    contadorZonas = 0
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
    cursor = conexion.cursor()
    listado_Tipos_Zona = []
    response_Tipos_Zona= requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/"+ str(aplication_id_gps)+"/geofencegroups", 
                        headers={"Authorization": token_id_gps}
                        )
    urlData = response_Tipos_Zona.content

    data_Tipos_Zona = json.loads(urlData)
    for x in data_Tipos_Zona :
        auxTipoZona = {'id':x['id'], 'nombre':x['name'], 'descripcion':x['description'], 'zonas':x['geofenceIds']}
        listado_Tipos_Zona.append(auxTipoZona)
    
    for x in listado_Tipos_Zona:
        for i in x['zonas']:
            response_Zonas = requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/"+str(aplication_id_gps)+"/geofences/"+ str(i), 
                    headers={"Authorization":token_id_gps}
                    )
            urlData = response_Zonas.content
            data_Zona = json.loads(urlData)
            aux_location_upload = []
            aux_location_upload.append(data_Zona['id'])
            aux_location_upload.append(data_Zona['name'])
            
            pit=6
            subcadena = data_Zona['name'][0:3]
            if(subcadena=='SOR'):
                pit=1
            elif(subcadena=='PGP'):
                pit=2
            elif(subcadena=='PGB'):
                pit=3
            elif(subcadena=='SAM'):
                pit=4
            elif(subcadena=='BOT'):
                pit=5
            else:
                pit=6
            try:
                now = datetime.datetime.now() + datetime.timedelta(hours=13)
                query_location_group = 'SELECT location_group_id from location_groups where location_group_name=?'
                cursor.execute(query_location_group, x['nombre'])
                result = cursor.fetchone()
                query_insert = 'insert into locations values (?,?,?,?,?,?,?,?,?,?,?,?)'
                cursor.execute(query_insert, 1,pit ,result[0],aux_location_upload[0],aux_location_upload[1],None, now, None,None,None,3,1)
                conexion.commit()
                print(data_Zona['name'])
                contadorZonas +=1
            except Exception as ex:
                query_select_delete = 'select * from locations where name=? and deleted_at is not null;'
                cursor.execute(query_select_delete, aux_location_upload[1])
                result = cursor.fetchone()
                
                if(result!=None):
                    print(result)
                    query_update_geo = '''UPDATE locations SET deleted_at=NULL where location_id=?'''
                    cursor.execute(query_update_geo,result[0])
                    conexion.commit()
                pass
            
    if(contadorZonas==0):
      print('No se agregaron nuevas zonas en la mina')
    else:
      print('Se agregaron ' +str(contadorZonas)+ ' nuevas zonas mineras')

def eliminarZonas(sql_connect, aplication_id_gps, token_id_gps):
    datetime_simberi = datetime.datetime.now() + datetime.timedelta(hours=13)
    list_geofences_bd = []
    list_geofences_system = []
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
    cursorDeleteZone = conexion.cursor()
    response_geofences= requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/"+str(aplication_id_gps)+"/geofences", 
                        headers={"Authorization":token_id_gps}
                        )
    urlData = response_geofences.content
    data_geofences = json.loads(urlData)
    query_select_location_bd = 'SELECT name from locations where deleted_at is null and location_id not in(54)'
    query_update_location_bd = 'UPDATE locations set deleted_at=? where name=?'
    cursorDeleteZone.execute(query_select_location_bd)
    result_locations = cursorDeleteZone.fetchall()
    for i in result_locations:
        list_geofences_bd.append(i[0])
    for x in data_geofences:
        list_geofences_system.append(x['name'])
    list_geofences_inactive = list(set(list_geofences_bd) - set(list_geofences_system))
    if(len(list_geofences_inactive)>0):
        for x in list_geofences_inactive:
            cursorDeleteZone.execute(query_update_location_bd,datetime_simberi,x)
            cursorDeleteZone.commit()
    else:
        print('no tiene datos')
    cursorDeleteZone.close()

def llenado_Tipos_Zonas(sql_connect, aplication_id_gps, token_id_gps):
    try:
        contador_Tipos_Zonas = 0
        contador_Zonas = 0
        conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
        cursor = conexion.cursor()
        listado_Tipos_Zona = []
        #CAMBIAR ID APLICACION
        response_Tipos_Zona= requests.get("https://simplefms.io/comGpsGate/api/v.1/applications/"+str(aplication_id_gps)+"/geofencegroups", 
                            headers={"Authorization":token_id_gps}
                            )
        urlData = response_Tipos_Zona.content
        data_Tipos_Zona = json.loads(urlData)
        for x in data_Tipos_Zona :
            try:
                auxTipoZona = {'id':x['id'], 'name':x['name'], 'descripcion':x['description'], 'zonas':x['geofenceIds']}
                listado_Tipos_Zona.append(auxTipoZona)
            except Exception as err:
                print(err)
        for x in listado_Tipos_Zona:
            #CHANGE QUERY LISTO?
            if(x['name']=='PGP' or x['name']=='PGB' or x['name']=='SOR' or x['name']=='SMT' or x['name']=='BTU'):
              print('No se debe ingresar')
            else:
              query = 'insert into location_groups values (?,?)'
              try:
                  cursor.execute(query, x['id'], x['name'])
                  conexion.commit()
                  contador_Tipos_Zonas += 1
              except Exception as err:
                  pass
        if(contador_Tipos_Zonas==0):
          print('No existen nuevos tipos de zona')
        else:
          print('Tipos de zonas nuevas agregadas= '+ str(contador_Tipos_Zonas))
        
        '''if(contador_Zonas == 0):
          print('No se agrego ninguna zona nueva')
        else:
          print('Zonas nuevas agregadas= ' + str(contador_Zonas))'''
        cursor.close()
        conexion.close()
    except Exception as err:
        print(err)

#tags
def ultimo_registro(sql_connect, aplication_id_gps, tag_truck_id, tag_shovel_id, token_id_gps):
    print('Entro a la revision del ultimo registro')
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
    cursor = conexion.cursor()
    response_loader = requests.get("http://83.229.5.28/comGpsGate/api/v.1/applications/"+str(aplication_id_gps)+"/tags/"+tag_shovel_id+"/users", 
        headers={"Authorization":token_id_gps})
    response_loader.status_code
    urlData = response_loader.content
    data_loader = json.loads(urlData)
    aux_equipos = []
    for x in data_loader:
        try:
            aux_pala={'id':x['id'], 'name':x['name']}
            aux_equipos.append(aux_pala)
        except Exception as err:
            print(err)

    response_trucks = requests.get("http://83.229.5.28/comGpsGate/api/v.1/applications/"+aplication_id_gps+"/tags/"+tag_truck_id+"/users",
                                    headers={"Authorization":token_id_gps})
    response_trucks.status_code
    urlData = response_trucks.content
    data_trucks = json.loads(urlData)
    aux_trucks = []
    for i in data_trucks:
        try:
            aux_camion = {'id':i['id'], 'name':i['name']}
            aux_equipos.append(aux_camion)
        except Exception as err:
            print(err)
    aux_posicion_final = []
    for i in aux_equipos:
        hora_conexion = None
        try:
            response_customfield = requests.get( "http://83.229.5.28/comGpsGate/api/v.1/applications/"+aplication_id_gps+"/users/"+str(i['id']), 
                                                headers={"Authorization":token_id_gps} )
            response_customfield.status_code
            urlData = response_customfield.content
            data_customfield = json.loads(urlData)
            df_customfield = pd.json_normalize( data_customfield)
            try:
                hora_conexion = datetime.datetime.strptime( df_customfield["trackPoint.utc"][0] , '%Y-%m-%dT%H:%M:%SZ') - timedelta(hours=6)
            except Exception as err:
                hora_conexion = datetime.datetime.strptime( df_customfield["trackPoint.utc"][0] , '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=6)
            #CHANGE QUERY LISTO?
            queryEquipos = 'select equipment_id from equipment where equipment_name=?'
            cursor.execute(queryEquipos,i['name'])
            result_Eq = cursor.fetchone()
            #print(result_Eq[0])
            try:
                #CHANGE QUERY LISTO?
                aa='''UPDATE last_positions set time_stamp= ? where equipment_lp_id=?
                        IF @@ROWCOUNT=0
                            INSERT INTO last_positions values (?,?) ;'''
                #CHANGE QUERY LISTO?
                query_Insert_Update='''INSERT INTO last_positions values (?,?) 
                                ON DUPLICATE KEY UPDATE time_stamp= ?'''
                #CHANGE QUERY LISTO?
                query_Insert_UP='insert into last_positions values(?,?)'
                cursor.execute(aa,hora_conexion,result_Eq[0],result_Eq[0],hora_conexion)
                cursor.commit()
                print(result_Eq[0])
                print(hora_conexion,result_Eq[0])
            except Exception as err:
                #print(err)
                pass
        except Exception as err:
            print(err)
            pass

def getEventsBeacons(initial, end, sql_connect):
    start_date = initial
    end_date = end
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
    cursor = conexion.cursor()
    response_trucks = requests.get("http://83.229.5.28/comGpsGate/api/v.1/applications/13/tags/153/users", headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"})
    response_trucks.status_code
    urlData = response_trucks.content
    data_trucks = json.loads(urlData)
    arrayEventsDay = []
    query_get_equipment_id = """SELECT equipment_id from equipment where equipment_name=?"""
    query_get_geofence_id = """SELECT * FROM locations where name=?"""

    while(start_date<=end_date):
        print(start_date)
        for i in data_trucks:
            userId = i['id']
            urlRequest = "http://83.229.5.28/comGpsGate/api/v.1/applications/13/events?FromIndex=0&PageSize=1000&Date="+ str(start_date) +"&UserId="+str(userId)+"&RuleId=253"
            response_events = requests.get(urlRequest, headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"})
            response_events.status_code
            urlData = response_events.content
            data_event_list = json.loads(urlData)
            #print(data_event_list)
            try:
                for data_beacon in data_event_list:
                    try:
                        if(data_beacon['ongoing']==False):
                            print(data_beacon)
                            initial_date_aux = data_beacon['boundingBox']['minTime']
                            end_date_aux = data_beacon['boundingBox']['maxTime']
                            #print(initial_date_aux)
                            initial_date_aux = datetime.datetime.strptime(initial_date_aux, '%Y-%m-%dT%H:%M:%SZ') - timedelta(hours=6)
                            end_date_aux =  datetime.datetime.strptime(end_date_aux, '%Y-%m-%dT%H:%M:%SZ') - timedelta(hours=6)
                            duration_aux =  end_date_aux - initial_date_aux
                            duration_minutes = duration_aux.seconds/60
                            arguments = data_beacon['arguments']
                            #print(arguments)
                            geofence_id = 54
                            if(arguments[3]['value']!='No Geofence'):
                                cursor.execute(query_get_geofence_id, arguments[3]['value'])
                                geofence_id = cursor.fetchone()[0]
                            cursor.execute(query_get_equipment_id, i['name']) 
                            truck_id =  cursor.fetchone()[0] 
                            shovel_id = None
                            if( arguments[0]['value']!='Workshop 1' or arguments[0]['value']!='Workshop 2' 
                            or arguments[0]['value']!='Refueling' or arguments[0]['value']!='BC_109'):
                                if(arguments[0]['value']=='EX645b' or arguments[0]['value']=='EX645a'):
                                    shovel_id = 645
                                else:
                                    cursor.execute(query_get_equipment_id, arguments[0]['value'])
                                    shovel_id = cursor.fetchone()[0]
                            auxDiccionario ={'id_event':data_beacon['id'],'equipment_name': i['name'],
                                            'start_date': initial_date_aux,
                                            'end_date': end_date_aux,
                                            'duration': duration_aux.seconds,
                                            'event_name':data_beacon['ruleName'], 'onGoing':data_beacon['ongoing'],
                                            'beacon_name': arguments[0]['value'],
                                            'beacon_level':arguments[1]['value'],
                                            'meters_level':arguments[2]['value'],
                                            'geofence_name':arguments[3]['value'],
                                            'geofence_id': geofence_id,
                                            'shovel_id': shovel_id,
                                            'truck_id': truck_id
                                            }
                            #print(auxDiccionario)
                            arrayEventsDay.append(auxDiccionario)
                    except Exception as err:
                        print(err)
                        pass
            except Exception as err:
                print(err)
                pass
        start_date = start_date + datetime.timedelta(days=1)
    cursor.close()   
    return arrayEventsDay

def get_beacon_information(start_date, end_date, sql_connect):
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
    cursor = conexion.cursor()
    events_beacons = getEventsBeacons(start_date, end_date, sql_connect)
    beacons_reports = pd.DataFrame(events_beacons)
    #filter_shovel = (beacons_reports['beacon_name'] == 'EX643') | (beacons_reports['beacon_name'] == 'EX534') | (beacons_reports['beacon_name'] == 'EX645a') | (beacons_reports['beacon_name'] == 'EX535') | (beacons_reports['beacon_name'] == 'EX597') | (beacons_reports['beacon_name'] == 'EX645b')
    #beacon_shovel = beacons_reports.loc[filter_shovel]
    query_insert_beacon = """INSERT INTO beacons_events VALUES(?,?,?,?,?,?,?,?,?);"""
    for row in events_beacons:
        try:
            print(row)
            cursor.execute(query_insert_beacon, row['id_event'], row['geofence_id'], row['truck_id'],
                           row['shovel_id'], row['beacon_level'], row['meters_level'], row['start_date'],
                           row['end_date'], row['duration'])
            cursor.commit()
        except Exception as err:
            print(err)
            pass
    cursor.close()   

def Request_Inputs(trucks_names, truck_last_activity, event_id, trucks_ids, event_name, automatic_event, event_day, sql_connect, aplication_id_gps, token_id_gps):
    
    cnxn_str = ("Driver={SQL Server};"
            "Server="+sql_connect[0] +";"
            "Database="+sql_connect[3]+";"
            "UID="+sql_connect[1]+";"
            "PWD="+sql_connect[2]+";")
    #region Read "in Blast" Events
    print('Reading Input '+event_name+' Signal Events')
    print('Truck    ',' |   ',' Start    ', '    |   ',' End  ','   |   ', 'On Going')
    for i in range(len(trucks_names)):
        print(trucks_names[i])
        truck_id_bd = Get_Data_BD.GetEquipmentIdByEquipmentName(sql_connect, trucks_names[i])
        print(truck_id_bd)
        list_already_writed  = None
        if(automatic_event):
            try:
                last_seen = datetime.datetime.strptime(truck_last_activity[i], '%Y-%m-%dT%H:%M:%S.%fZ') - datetime.timedelta(hours=6)
            except:
                last_seen = datetime.datetime.strptime(truck_last_activity[i], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
        else:
            try:
                print(event_day)
                last_seen = event_day
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)

        try:
            response_events_rend = requests.get("http://83.229.5.28/comGpsGate/api/v.1/applications/"+str(aplication_id_gps)+"/events?FromIndex=0&PageSize=1000&Date="+ last_seen.strftime('%Y-%m-%d') +"&UserId="+str(trucks_ids[i])+"&RuleId="+ str(event_id), 
                                                headers={"Authorization":token_id_gps})
            response_events_rend.status_code
            urlData = response_events_rend.content
            data_event_list_rend = json.loads(urlData)
            data_event_list_rend = pd.json_normalize(data_event_list_rend)
            print('Se van a imprimir los eventos de backing')
            print(data_event_list_rend)

            try:
                cnxn   = pyodbc.connect(cnxn_str)
                cursor = cnxn.cursor()
            
                query = "SELECT "+event_name+"_id FROM event_"+event_name+" WHERE equipment_id = "+str(truck_id_bd)+" AND time_end BETWEEN '"+last_seen.strftime('%Y-%m-%d')+" 00:00:00' AND '"+last_seen.strftime('%Y-%m-%d')+" 23:59:59';"
                print(query)
                data_already_writed = pd.read_sql(query, cnxn)
                list_already_writed = data_already_writed[event_name+'_id'].values.tolist()

            except Exception as err:
                print('Fallo A')
                print(err)
                pass
           

            for j in range(len(data_event_list_rend)):
                try:
                    #print(j)
                    timestamp_end = datetime.datetime.strptime(data_event_list_rend['boundingBox.maxTime'][j], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
                    timestamp_start = datetime.datetime.strptime(data_event_list_rend['boundingBox.minTime'][j], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
                    
                    truck_id    = data_event_list_rend['userId'][j]
                    #FALTA PONER VARIABLE EN EVENTO PARA EL NOMBRE DE LA GEOCERCA.
                    #location_id_bd = Get_Data_BD.GetLocationIdByLocationName(sql_connect, ) 
                    
                    
                    reg_index = timestamp_start.strftime('%Y%m%d%H%M%S') + str(truck_id_bd)

                    if (data_event_list_rend['ongoing'][j] == False and reg_index not in list_already_writed):
                        print( trucks_names[i], '    |   ',timestamp_start,'   |   ', timestamp_end,'   |   ', data_event_list_rend['ongoing'][j] )
                        #print(timestamp_start.strftime('%Y%m%d%H%M%S') )

                        #-------------------- Write SQL Server -----------------------------
                        try:
                            cnxn   = pyodbc.connect(cnxn_str)
                            cursor = cnxn.cursor()

                            total_seconds = timestamp_end - timestamp_start
                            total_seconds = total_seconds.seconds

                            
                            print('Segundos')
                            print(total_seconds)

                            query_insert_data = ("INSERT INTO event_"+event_name+"("+event_name+"_id, equipment_id, time_start, time_end, durations) VALUES ("+timestamp_start.strftime('%Y%m%d%H%M%S') + str(truck_id_bd) + ", "+str(truck_id_bd)+", '"+str(timestamp_start)+"', '"+str(timestamp_end)+"', "+str(total_seconds)+");")

                            print(query_insert_data)
                            cursor.execute(query_insert_data)
                            cnxn.commit()
                            del cnxn

                            #print("Writing to SQL Server")
                        except Exception as e:
                            print('Fallo b')
                            print(e)
                            pass
                            #print('Error de Conexión: '  + str(e))

                except Exception as e:
                    print('Fallo C')
                    print(e)
                    pass
        except Exception as err:
            print('Los equipos no tienen informacion')
            print(err)
    print('\n')
    #endregion
   
def Request_GTS(trucks_names, truck_last_activity, event_id, trucks_ids, automatic_event, event_day, sql_connect):
    cnxn_str = ("Driver={SQL Server};"
            "Server="+sql_connect[0] +";"
            "Database="+sql_connect[3]+";"
            "UID="+sql_connect[1]+";"
            "PWD="+sql_connect[2]+";")
    #region Read "in Blast" Events
    print('Reading "GTS" Events')
    print('Truck    ',' |   ','Geofence',' |   ','Group', '   |   ',' Start    ', '    |   ',' End  ','   |   ', 'On Going')
    for i in range(len(trucks_names)):
        #print(trucks_names[i])
        if(automatic_event):
            try:
                last_seen = datetime.datetime.strptime(truck_last_activity[i], '%Y-%m-%dT%H:%M:%S.%fZ') - datetime.timedelta(hours=6)
            except:
                last_seen = datetime.datetime.strptime(truck_last_activity[i], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
        else:
            last_seen = event_day
        try:
            response_events_rend = requests.get("http://83.229.5.28/comGpsGate/api/v.1/applications/13/events?FromIndex=0&PageSize=1000&Date="+ last_seen.strftime('%Y-%m-%d') +"&UserId="+str(trucks_ids[i])+"&RuleId="+ str(event_id), headers={"Authorization":"OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB"})
            response_events_rend.status_code
            urlData = response_events_rend.content
            data_event_list_rend = json.loads(urlData)
            data_event_list_rend = pd.json_normalize(data_event_list_rend)

            try:
                cnxn   = pyodbc.connect(cnxn_str)
                cursor = cnxn.cursor()
            
                query = "SELECT [index] FROM event_in_geofence WHERE truck_name = '"+trucks_names[i]+"' AND time_end BETWEEN '"+last_seen.strftime('%Y-%m-%d')+" 00:00:00' AND '"+last_seen.strftime('%Y-%m-%d')+" 23:59:59';"
                data_already_writed = pd.read_sql(query, cnxn)
                list_already_writed = data_already_writed['index'].values.tolist()

            except:
                pass


            for j in range(len(data_event_list_rend)):
                try:
                    #print(j)
                    df_args_rend = pd.json_normalize(data_event_list_rend['arguments'][j])
                    timestamp_end = datetime.datetime.strptime(data_event_list_rend['boundingBox.maxTime'][j], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
                    timestamp_start = datetime.datetime.strptime(data_event_list_rend['boundingBox.minTime'][j], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
                    
                    truck_id    = data_event_list_rend['userId'][j]
                    geofence    = df_args_rend['value'][0]
                    geogroup    = df_args_rend['value'][1]


                    
                    reg_index = timestamp_start.strftime('%Y%m%d%H%M%S') + str(truck_id)

                    if (data_event_list_rend['ongoing'][j] == False and reg_index not in list_already_writed):
                        print( trucks_names[i], '    |   ', geofence, '    |   ', geogroup, '    |   ',timestamp_start,'   |   ', timestamp_end,'   |   ', data_event_list_rend['ongoing'][j] )
                        #print(timestamp_start.strftime('%Y%m%d%H%M%S') )
                        #-------------------- Write SQL Server -----------------------------
                        try:
                            cnxn   = pyodbc.connect(cnxn_str)
                            cursor = cnxn.cursor()

                            query_insert_data = ("INSERT INTO event_in_geofence\
                                                        ([index],truck_name, geofence, geofence_group, time_start, time_end, duration)\
                                                    VALUES ('"+ reg_index + "', '"+trucks_names[i]+"', '"+geofence+"', '"+geogroup+"', '"+str(timestamp_start)+"', '"+str(timestamp_end)+"', (SELECT DATEDIFF(second, '"+str(timestamp_start)+"', '"+str(timestamp_end)+"')));")

                            #print(query_insert_data)
                            cursor.execute(query_insert_data)
                            cnxn.commit()
                            del cnxn

                            #print("Writing to SQL Server")
                        except Exception as e:   
                            #print(query_insert_data)
                            #print('Error de Conexión: '  + str(e))
                            pass

                except Exception as e:
                    print(e)
                    pass
        except Exception as err:
            print('Error al recolectar eventos GTS')
            print(err)
    print('\n')
    #endregion

def Request_GTS_Stop(trucks_names, truck_last_activity, event_id, trucks_ids, automatic_event, event_day, sql_connect, aplication_id, token_id_gps):
    cnxn_str = ("Driver={SQL Server};"
            "Server="+sql_connect[0] +";"
            "Database="+sql_connect[3]+";"
            "UID="+sql_connect[1]+";"
            "PWD="+sql_connect[2]+";")
    #region Read "in Blast" Events
    print('Reading "GTS Stop" Events')
    print(truck_last_activity)
    print('Truck    ',' |   ','Geofence',' |   ','Group', '   |   ',' Start    ', '    |   ',' End  ','   |   ', 'On Going')
    
    for i in range(len(trucks_names)):
        print(trucks_names[i])
        list_already_writed = None
        if(automatic_event):
            try:
                last_seen = datetime.datetime.strptime(truck_last_activity[i], '%Y-%m-%dT%H:%M:%S.%fZ') - datetime.timedelta(hours=6)
            except:
                last_seen = datetime.datetime.strptime(truck_last_activity[i], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
        else:
            last_seen = event_day
        
        try:
            response_events_rend = requests.get("http://83.229.5.28/comGpsGate/api/v.1/applications/"+str(aplication_id)+"/events?FromIndex=0&PageSize=1000&Date="+ last_seen.strftime('%Y-%m-%d') +"&UserId="+str(trucks_ids[i])+"&RuleId="+ str(event_id), headers={"Authorization":token_id_gps})
            response_events_rend.status_code
            urlData = response_events_rend.content
            data_event_list_rend = json.loads(urlData)
            data_event_list_rend = pd.json_normalize(data_event_list_rend)
            print(data_event_list_rend)
            try:
                cnxn   = pyodbc.connect(cnxn_str)
                cursor = cnxn.cursor()
            
                query = "SELECT [index] FROM event_stop_in_geofence WHERE truck_name = '"+trucks_names[i]+"' AND time_end BETWEEN '"+last_seen.strftime('%Y-%m-%d')+" 00:00:00' AND '"+last_seen.strftime('%Y-%m-%d')+" 23:59:59';"
                data_already_writed = pd.read_sql(query, cnxn)
                print(data_already_writed)
                list_already_writed = data_already_writed['index'].values.tolist()

            except Exception as err:
                print(err)
                pass
                
           

            for j in range(len(data_event_list_rend)):
                try:
                    #print(j)
                    df_args_rend = pd.json_normalize(data_event_list_rend['arguments'][j])
                    timestamp_end = datetime.datetime.strptime(data_event_list_rend['boundingBox.maxTime'][j], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
                    timestamp_start = datetime.datetime.strptime(data_event_list_rend['boundingBox.minTime'][j], '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(hours=6)
                    
                    truck_id    = data_event_list_rend['userId'][j]
                    geofence    = df_args_rend['value'][0]
                    geogroup    = df_args_rend['value'][1]
                    odometerDistance = 0
                    try:
                        odometerDistance = df_args_rend['value'][4]
                        #print('La distancia del odometro es: ', odometerDistance)
                    except Exception as err:
                        print('error 3')
                        print(err)

                    
                    
                    reg_index = timestamp_start.strftime('%Y%m%d%H%M%S') + str(truck_id)

                    if (data_event_list_rend['ongoing'][j] == False and reg_index not in list_already_writed):
                        print( trucks_names[i], '    |   ', geofence, '    |   ', geogroup, '    |   ',timestamp_start,'   |   ', timestamp_end,'   |   ', data_event_list_rend['ongoing'][j] )
                        #print(timestamp_start.strftime('%Y%m%d%H%M%S') )

                        #-------------------- Write SQL Server -----------------------------
                        try:
                            cnxn   = pyodbc.connect(cnxn_str)
                            cursor = cnxn.cursor()

                            query_insert_data = ("INSERT INTO event_stop_in_geofence\
                                                        ([index],truck_name, geofence, geofence_group, time_start, time_end, duration, last_odometer_in_meters)\
                                                    VALUES ("+timestamp_start.strftime('%Y%m%d%H%M%S') + str(truck_id) + ", '"+trucks_names[i]+"', '"+geofence+"', '"+geogroup+"', '"+str(timestamp_start)+"', '"+str(timestamp_end)+"', (SELECT DATEDIFF(second, '"+str(timestamp_start)+"', '"+str(timestamp_end)+"')),"+odometerDistance+");")

                            #print(query_insert_data)
                            cursor.execute(query_insert_data)
                            cnxn.commit()
                            del cnxn

                            #print("Writing to SQL Server")
                        except Exception as e:
                            pass
                            #print('Error de Conexión: '  + str(e))

                except Exception as e:
                    print(e)
                    pass
        except Exception as err:
            print('Error al capturar eventos GTS Stop Odometer')
    print('\n')
    #endregion
    
def Read_Events(event_date, automatic_event,sql_connect, aplication_id, tag_truck_id, token_id_gps):
    print('Reading Event Trucks...' )
    response_trucks = requests.get("http://83.229.5.28/comGpsGate/api/v.1/applications/"+aplication_id+"/tags/"+tag_truck_id+"/users", 
                                   headers={"Authorization":token_id_gps})
    response_trucks.status_code
    urlData = response_trucks.content
    data_trucks = json.loads(urlData)
    df_trucks = pd.json_normalize( data_trucks )

    trucks_last_activity = df_trucks['trackPoint.utc']
    trucks_names = df_trucks["name"]
    trucks_ids = df_trucks["id"]
    #Request_Inputs(trucks_names,trucks_last_activity, 173, trucks_ids, 'tipping', automatic_event,event_date,sql_connect, aplication_id, token_id_gps)
    #backing fm-130
    Request_Inputs(trucks_names,trucks_last_activity, 741, trucks_ids, 'backing', automatic_event,event_date,sql_connect, aplication_id, token_id_gps)
    #backing fm-650
    #Request_Inputs(trucks_names,trucks_last_activity, 313, trucks_ids, 'backing', automatic_event,event_date,sql_connect, aplication_id, token_id_gps)
    #Request_Inputs(trucks_names,trucks_last_activity, 179, trucks_ids, 'call_dispatch', automatic_event, event_date,sql_connect, aplication_id, token_id_gps)
    Request_GTS_Stop(trucks_names,trucks_last_activity, 725,  trucks_ids, automatic_event, event_date,sql_connect, aplication_id, token_id_gps)
    Request_GTS_Stop(trucks_names,trucks_last_activity, 726,  trucks_ids, automatic_event, event_date,sql_connect, aplication_id, token_id_gps)
    #Request_GTS(trucks_names,trucks_last_activity, 161,  trucks_ids, automatic_event, event_date,sql_connect)
    #Request_GTS(trucks_names,trucks_last_activity, 171,  trucks_ids, automatic_event, event_date,sql_connect)

def main_read_events(initial_date, end_date, manual_event,sql_connect, aplication_id, tag_truck_id, token_id):
        try:
            print('El sistema entro a la recoleccion de eventos')
            if(manual_event):
                print('modo manual')
                flag_initial_day = datetime.datetime.fromisoformat(initial_date)
                flag_initial_day = flag_initial_day.replace(minute=0, hour=0, second=1)
                flag_end_day = datetime.datetime.fromisoformat(end_date)
                
                while(flag_initial_day<=flag_end_day):
                    print(flag_initial_day)
                    Read_Events(flag_initial_day,False, sql_connect, aplication_id, tag_truck_id, token_id)
                    flag_initial_day = flag_initial_day + datetime.timedelta(days=1)
                    print('El sistema termino de capturar los eventos manualmente')
                return 1
                
            else:
                print('modo modo automatico')
                Read_Events(datetime.datetime.fromisoformat(initial_date),True,sql_connect, aplication_id, tag_truck_id, token_id)
                return 2
        except Exception as err:
            print('fallo la recoleccion de eventos')
            print(err)

