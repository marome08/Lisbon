import warnings
import pandas as pd
import numpy as np
from datetime import datetime
import  datetime
import socket
import PySimpleGUI as sg
import Alternative_function
import Get_API_Data
import Procesing_Report_Travel
import Reconciler_Summary_Travels
import ReconcilerTumRecords
import Reconciler_Distance_Travels
import Get_Data_BD
import AlertErrorTUM
import SummaryProductionByWebPage
import pyodbc
warnings.simplefilter('ignore')
import traceback
 

df_load_cycles = pd.DataFrame()
df_load_cycles_Simberi = pd.DataFrame()
first_join = True
limit_time_stamp = None
limit_is_reported_time_stamp = None

#Se agrega una variable para cambiar el valor de is_reported
def New_Load_to_Database(sql_connect, df_load_cycles, limit_is_reported_timestamp, gmt_hour_zone):
  global new_reg_load
  created_at_UTC = datetime.datetime.utcnow()
  is_reported = 1
  #updatedASimberi = datetimeSimberiAux.strftime("%d-%m-%Y %H:%M:%S")
  created_at_simberi = created_at_UTC + datetime.timedelta(hours=gmt_hour_zone)

  conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2])
  cursor = conexion.cursor()
  #Se agrega columna material_class_id
  query_pala_update='''INSERT INTO obs_travels values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
  #Se agrego el created_at, el is_reported y material_class_id.
  query_insert_rv = 'insert into travels values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
  new_reg_load = 0
  deleted_to_improve = 0
  primary_key_setted = False
  primary_key_setted_already = False
  contadorCorrectos = 0
  contadorIncorrectos = 0
  contadorCargado = 0
  contadorVacio = 0
  contadorAobservar = 0
  contadorOtro = 0

  df_load_cycles['Equipo'] = df_load_cycles['Equipo'].astype(int).astype(str)
  for i in df_load_cycles.index:
    try:
        error_type_cases = 101
        reason_id = None
        destino_original = None
        destino_subido = None
        origen_original = None
        origen_subido = None
        material_original = 4
        material_subido = material_original
        material_block_original = 1
        material_block_subido = material_block_original
        cargador_original = 90909
        cargador_subido = cargador_original
        trayecto_original = None
        trayecto_subido = None
        categoria_origen_subido = None
        categoria_destino_subido = None
        flag_estimador_t = 0
        flag_estimador_mix = 0
        flag_estimador_mat = 0
        flag_sin_pala_material = 1
        now = datetime.datetime.now()
        block_name_original = 1
        block_name_subido = block_name_original
        materialBlockAux = 'No_Block_Name'
        material_class_id_original = 1
        material_class_id_subido = material_class_id_original
        gradeAux = 0
        gradeAuxSul = 0
        rehandleAux = 0
        id = i
        case_traveling = df_load_cycles['case'][i]
        print(case_traveling)
        if(case_traveling!=1):
           messageLogCase = "El siguiente id fue mejorado a traves del caso: " + str(case_traveling) + " y del ID: " + str(id)
           Alternative_function.create_log_file("traveling_id.log",messageLogCase)
        tst = df_load_cycles['travel_same_type'][i]
        td = df_load_cycles['time_depart'][i]
        time_origin_start = df_load_cycles['time_origin_start'][i]
        inside_time_origin = df_load_cycles['inside_time_origin'][i]
        ta = df_load_cycles['time_arrive'][i]
        tpd = df_load_cycles['time_production_depart'][i]
        tpa = df_load_cycles['time_production_arrive'][i]
        tsas = df_load_cycles['time_spotting_and_spotted'][i]
        tpsas = df_load_cycles['time_production_spotting_and_spotted'][i]    
        tps = df_load_cycles['time_production_stamp'][i]
        tt = df_load_cycles['traveling_time'][i]
        it = df_load_cycles['inside_time'][i]
        tur = df_load_cycles['Turno'][i]
        trayecto_original = df_load_cycles['Tipo_trayecto'][i]
        cua = df_load_cycles['Cuadrilla'][i]
        ts = df_load_cycles['time_stamp'][i]
        travelDistance = df_load_cycles['travel_distance'][i]
        operator_id = 20
        origen_original = df_load_cycles['Blast_id'][i]
        destino_original = df_load_cycles['Dump_id'][i]
        categoria_origen_original = df_load_cycles['Categoria_origen'][i]
        categoria_destino_original = df_load_cycles['Categoria_destino'][i]
        queryPayload='select em.bcm from equipment_models em, equipment eq where eq.equipment_model_id=em.equipment_model_id AND equipment_name=?;'
        cursor.execute(queryPayload,df_load_cycles['Equipo'][i])
        resultPayload = cursor.fetchall()
        payload = resultPayload[0][0]
        #ton = payload
        ton=90

        if(ts<limit_is_reported_timestamp):
           is_reported = 0

        try:
            eq_trans = Get_Data_BD.GetEquipmentIdByEquipmentName(sql_connect, df_load_cycles['Equipo'][i])
            operator_id = Get_Data_BD.GetOperatorIdByTravel(sql_connect, eq_trans, ts)
            reason_id =  Get_Data_BD.GetReasonIdByEquipmentId(sql_connect, eq_trans, ts)
        except Exception as err:
            print(err)
            print('error en obtencion datos')
            pass

        start_date_origin = time_origin_start
        end_date_origin = td
        start_date_destination = ta
        end_date_destination = ts

        ori_start_date_backing = None
        ori_end_date_backing = None
        ori_duration_backing = None
        ori_total_backing_event = None
        ori_backing_events_list = None

        des_start_date_backing = None
        des_end_date_backing = None
        des_duration_backing = None
        des_total_backing_event = None
        des_backing_events_list = None

        ori_start_date_tipping = None
        ori_end_date_tipping = None
        ori_duration_tipping = None
        ori_total_tipping_event = None
        ori_tipping_events_list = None

        des_start_date_tipping = None
        des_end_date_tipping = None
        des_duration_tipping = None
        des_total_tipping_event = None
        des_tipping_events_list = None

        (ori_start_date_backing, ori_end_date_backing, ori_duration_backing,
         ori_total_backing_event, ori_backing_events_list) = Get_Data_BD.Get_Backing_Or_Tipping_Signal(eq_trans, start_date_origin,
                                                                                                           end_date_origin, sql_connect, 'backing')    
        (des_start_date_backing, des_end_date_backing, des_duration_backing,
            des_total_backing_event, des_backing_events_list) = Get_Data_BD.Get_Backing_Or_Tipping_Signal(eq_trans, start_date_destination,
                                                                                                           end_date_destination, sql_connect, 'backing')
        
        (ori_start_date_tipping, ori_end_date_tipping, ori_duration_tipping,
            ori_total_tipping_event, ori_tipping_events_list) = Get_Data_BD.Get_Backing_Or_Tipping_Signal(eq_trans, start_date_origin,
                                                                                                           end_date_origin, sql_connect, 'tipping')
        (des_start_date_tipping, des_end_date_tipping, des_duration_tipping,
            des_total_tipping_event, des_tipping_events_list) = Get_Data_BD.Get_Backing_Or_Tipping_Signal(eq_trans, start_date_destination,
                                                                                                           end_date_destination,sql_connect, 'tipping')
        try:
          #CHANGE QUERY LISTO?
          origen_original = Get_Data_BD.GetLocationIdByLocationName(sql_connect, df_load_cycles['Blast'][i])
          destino_original = Get_Data_BD.GetLocationIdByLocationName(sql_connect, df_load_cycles['Dump'][i])
          destino_subido = destino_original
          origen_subido = origen_original         
        except Exception as ex:
          print('Error de select', ex)
          print(df_load_cycles['Blast'][i])
          print(df_load_cycles['Blast_id'][i])
          print(df_load_cycles['Dump'][i])
          print(df_load_cycles['Dump_id'][i])
        

        #VERIFICA QUE LOS VIAJES ESTAN CARGADOS O A OBSERVAR
        if(trayecto_original==1):

          #------------------ASIGNACION MATERIAL - POSTURA ------------------
          #Se busca caso 1 
          query_mat_car = '''select TOP 1 equipment_c_id, material_id, pit_level_material_id, grade, 
                              material_block_name, rehandle, sulphu, block_names_record_id, material_class_id
                                    from productive_routes
                                    where blast_location_id = ? and destination_location_id = ? and
                                    initial_date<=?
                                    and (end_date >? or end_date is null) AND is_active=1
                                    order by initial_date desc;'''
          cursor.execute(query_mat_car,origen_original,destino_subido, ts, ts)
          result_Car_Mar = cursor.fetchall()
          if(len(result_Car_Mar)>0):
            #Se encontro la postura
            #Corresponde al CASO-101
            error_type_cases = 101
            cargador_subido = result_Car_Mar[0][0]
            material_subido = result_Car_Mar[0][1]
            material_block_subido = result_Car_Mar[0][2]
            gradeAux = result_Car_Mar[0][3]
            materialBlockAux = result_Car_Mar[0][4]
            block_name_subido = result_Car_Mar[0][7]
            material_class_id_subido =  result_Car_Mar[0][8]
            rehandleAux = result_Car_Mar[0][5]
            gradeAuxSul = result_Car_Mar[0][6]
            flag_sin_pala_material=0
            if(rehandleAux==None):
                rehandleAux=0
          if(flag_sin_pala_material==1):
            query_pr_no_active = '''select TOP 1 equipment_c_id, material_id, pit_level_material_id, grade, 
                material_block_name, rehandle, sulphu, block_names_record_id, material_class_id
                from productive_routes
                where blast_location_id = ? and destination_location_id = ? and
                initial_date<=?
                and (end_date >? or end_date is null)
                order by initial_date desc;'''
            cursor.execute(query_pr_no_active,origen_original,destino_subido, ts, ts)
            result_pr_no_active = cursor.fetchall()
            if(len(result_pr_no_active)>0):
                #Corresponde al caso-103
                print('Encontro Pr no activa')
                error_type_cases = 103
                cargador_subido = result_pr_no_active[0][0]
                material_subido = result_pr_no_active[0][1]
                material_block_subido = result_pr_no_active[0][2]
                gradeAux = result_pr_no_active[0][3]
                materialBlockAux = result_pr_no_active[0][4]
                block_name_subido = result_pr_no_active[0][7]
                material_class_id_subido =  result_pr_no_active[0][8]
                rehandleAux = result_pr_no_active[0][5]
                gradeAuxSul = result_pr_no_active[0][6]
                if(rehandleAux==None):
                  rehandleAux=0
            else:
              query_get_shovel_beacon = """SELECT * FROM beacons_events where truck_id=? AND
                                          start_date>=? AND end_date<=? AND location_id=?;"""

              cursor.execute(query_get_shovel_beacon, eq_trans, time_origin_start, td, origen_original)
              result_beacon = cursor.fetchall()
              if(len(result_beacon)>0):
                #ver si la pala estaba operativa
                query_get_operating = """SELECT * FROM view_hist_states 
                                          where equipment_id=? and
                                          time_category_name='Operating' and start_date<=? and 
                                          (end_date>=? or end_date is null)
                                      """
                print('Se encontro el beacon')
                print(result_beacon)
                print(result_beacon[0])
                shovel_beacon = result_beacon[0][3]
                start_date_beacon = result_beacon[0][6]
                end_date_beacon = result_beacon[0][7]
                cursor.execute(query_get_operating,shovel_beacon, start_date_beacon, end_date_beacon)
                resul_operating = cursor.fetchall()
                aux_resul_operating = 1
                #if(aux_resul_operating==1):
                if(len(resul_operating)>0):
                  #Corresponde al caso-104
                  error_type_cases = 104
                  print('BEACON OPERATIVO')
                  messageLog = "el viaje con ID " + str(id) + " tuvo un beacon"
                  Alternative_function.create_log_file('travel_beacon.log',messageLog)
                  cargador_subido = result_beacon[0][3]
                else:  
                  print('No se encontro el beacon 2')
                  query_mat_car2 = '''SELECT TOP 1 equipment_c_id
                                      from productive_routes
                                      where blast_location_id = ? AND
                                      initial_date<=?
                                      AND (end_date >? OR end_date is null) AND is_active=1
                                      order by initial_date desc;'''
                  cursor.execute(query_mat_car2,origen_original, ts, ts)
                  result_Car_Mar2 = cursor.fetchall()
                  if(len(result_Car_Mar2)==0):
                    #Corresponde al caso-106
                    error_type_cases = 106
                    messageLog = "el viaje con ID " + str(id) + " no tenia productive routes y tampoco tuvo un beacon"
                    cargador_original = 90909
                    material_original = 4
                    material_block_original = 1
                    material_subido = material_original
                    cargador_subido = cargador_original
                    material_block_subido = material_block_original
                    block_name_subido = block_name_original
                    material_class_id_subido = material_class_id_original

                    flag_sin_pala_material = 1
                    Alternative_function.create_log_file('sin_pr_sin_beacon.log',messageLog)
                    print('Sin postura')
                  else:
                    #Corresponde al caso-105
                    error_type_cases = 5
                    cargador_subido = result_Car_Mar2[0][0]
                    print('Sin postura, pero con camion y sin material')
              else:
                print('No se encontro el beacon')
                query_mat_car2 = '''SELECT TOP 1 equipment_c_id
                                      from productive_routes
                                      where blast_location_id = ? AND
                                      initial_date<=?
                                      AND (end_date >? OR end_date is null) AND is_active=1
                                      order by initial_date desc;'''
                
                cursor.execute(query_mat_car2,origen_original, ts, ts)
                result_Car_Mar2 = cursor.fetchall()
                if(len(result_Car_Mar2)==0):
                  #Corresponde al caso-106
                  error_type_cases = 106
                  cargador_original = 90909
                  material_original = 4
                  material_block_original = 1
                  material_subido = material_original
                  cargador_subido = cargador_original
                  material_block_subido = material_block_original
                  block_name_subido = block_name_original
                  material_class_id_subido = material_class_id_original
                  flag_sin_pala_material = 1
                  print('Sin postura')
                else:
                  #Corresponde al caso-105
                  error_type_cases = 105
                  cargador_subido = result_Car_Mar2[0][0]
                  print('Sin postura, pero con camion y sin material')
              #1 - Primero manda el Beacon
            print(origen_original)
            print(time_origin_start)
            print(td)
            print(eq_trans)
            
            #2 - Si no existe el Evento de Beacon se ocupa el Productive Routes pero solo de la pala
            print('Viajes con error o sin material y pala')
            #------------------SE INSERTAN LOS VIAJES CARGADOS Y A OBSERVAR QUE PASARON POR EL ESTIMADOR------------------
            try:
              #CHANGE QUERY LISTO?
              cursor.execute(query_insert_rv, 
                                id, destino_subido,origen_original,3,cua, operator_id, cargador_subido, eq_trans, 
                                material_subido, tst,td,ta, tpd,tpa,tsas,tpsas,ts,tps,tt,it,tur,ton, categoria_origen_original,
                                categoria_destino_original, material_block_subido, gradeAux, materialBlockAux, rehandleAux, gradeAuxSul,
                                time_origin_start, inside_time_origin, travelDistance, block_name_subido, reason_id, created_at_simberi, is_reported, material_class_id_subido,
                                ori_start_date_backing, ori_end_date_backing, des_start_date_backing, des_end_date_backing,
                                ori_start_date_tipping, ori_end_date_tipping, des_start_date_tipping, des_end_date_tipping
                                )
              conexion.commit()
              new_reg_load += 1
              try:
                #CHANGE QUERY LISTO? 
                cursor.execute(query_pala_update,id, destino_original,origen_original, cargador_original, eq_trans,material_original,error_type_cases,3,now,None,None,
                                'System',None,None,None,None,None,material_block_original,None,rehandleAux, None, block_name_original, None, material_class_id_original, None
                                )
                conexion.commit()
                if(destino_original!=destino_subido):
                    #CHANGE QUERY LISTO?
                    queryDES = '''UPDATE obs_travels set  
                                new_destination_id=? where travel_id=? and updated_by='System';'''
                    cursor.execute(queryDES, destino_subido, id)

                if(cargador_original!=cargador_subido or cargador_subido==90909):
                    #CHANGE QUERY LISTO?
                    queryCAR = '''UPDATE obs_travels set  
                                new_equipment_c_id=? where travel_id=? and updated_by='System';'''
                    cursor.execute(queryCAR, cargador_subido, id)

                if(material_original!=material_subido or material_subido==4):
                    #CHANGE QUERY LISTO?
                    queryMAT = '''UPDATE obs_travels set new_material_id=?, new_pit_material_level=?, new_block_names_record_id=?, material_class_id_new=?  
                                where travel_id=? and updated_by='System';'''
                    cursor.execute(queryMAT, material_subido,material_block_subido, block_name_subido, material_class_id_subido, id)
                conexion.commit()
              except Exception as err:
                print('Error auditoria 1')
                print(err)
                pass 
            except Exception as err:
              print('Error viajes cargados Beacon')
              print(err)
              pass
            contadorIncorrectos+=1
          else:
            #------------------SE INSERTAN LOS VIAJES CORRECTOS------------------
            if(tt<0.3 and trayecto_original==1):
              #Corresponde al caso-102
              error_type_cases = 102
              print('Estos viajes son TBC POR TIEMPO DE TRAYECTO MUY CORTO')
              try:
                cursor.execute(query_insert_rv, 
                                  id, destino_subido,origen_original, 3,cua, operator_id, cargador_subido, eq_trans, 
                                  material_subido, tst,td,ta, tpd,tpa,tsas,tpsas,ts,tps,tt,it,tur,ton, categoria_origen_original,
                                  categoria_destino_original, material_block_subido, gradeAux,materialBlockAux, rehandleAux, gradeAuxSul,
                                  time_origin_start, inside_time_origin, travelDistance, block_name_subido, reason_id, created_at_simberi, is_reported, material_class_id_subido,
                                  ori_start_date_backing, ori_end_date_backing, des_start_date_backing, des_end_date_backing,
                                  ori_start_date_tipping, ori_end_date_tipping, des_start_date_tipping, des_end_date_tipping
                                  )
               
                cursor.execute(query_pala_update,id, destino_subido,origen_original, cargador_subido, eq_trans,material_subido,error_type_cases,3,now,None,None,
                                'System',None,None,None,None,None,material_block_subido,None,rehandleAux, None, block_name_subido, None, material_class_id_subido, None
                                )
                conexion.commit()
                new_reg_load += 1
              except Exception as err:
                print('ERROR AL INSERTAR TBC TIME')
                print(err)
                pass
              
            else:
              #Corresponde al CASO-101
              error_type_cases = 101
              try:
                #CHANGE QUERY LISTO?
                cursor.execute(query_insert_rv, 
                                id, destino_subido,origen_original, trayecto_original,cua, operator_id, cargador_subido, eq_trans, 
                                material_subido, tst,td,ta, tpd,tpa,tsas,tpsas,ts,tps,tt,it,tur,ton, categoria_origen_original,
                                categoria_destino_original, material_block_subido, gradeAux,materialBlockAux, rehandleAux, gradeAuxSul,
                                time_origin_start, inside_time_origin, travelDistance, block_name_subido, reason_id, created_at_simberi, is_reported, material_class_id_subido,
                                ori_start_date_backing, ori_end_date_backing, des_start_date_backing, des_end_date_backing,
                                ori_start_date_tipping, ori_end_date_tipping, des_start_date_tipping, des_end_date_tipping
                                )
                conexion.commit()
                new_reg_load += 1
              except Exception as err:
                print('Error al insertar viaje correcto')
                print(err)
                pass
            
            contadorCorrectos+=1
        elif(trayecto_original==3):
            print('TBC')
            #Corresponde al caso-204
            error_type_cases = 204
            #------------------ESTIMADOR T------------------
            if(categoria_origen_original=='Blasts' and origen_original==destino_original):
                queryDestino ='''SELECT TOP 1 tr.destination_location_id, COUNT(tr.travel_id) as total_travels
                      FROM travels tr
                      INNER JOIN locations ld ON tr.destination_location_id=ld.location_id 
                      where DATEDIFF(hh, tr.departed_time , DATEADD(hh, 13, GETDATE()))<=5
                      AND tr.origin_category='Blasts'
                      AND ld.location_group_id IN (3,5,4)
                      AND tr.origin_location_id=?
                      group by tr.destination_location_id
                      ORDER BY total_travels DESC;'''
                cursor.execute(queryDestino,origen_original)
                resultDestino = cursor.fetchone()
                if(resultDestino!=None):
                    destino_subido = resultDestino[0]
                    flag_estimador_t = 1
                    print("Actuo estimador T")
            #------------------ASIGNACION POSTURA DESPUES DE ESTIMADORES------------------
            #CHANGE QUERY LISTO?
            query_mat_car = '''select TOP 1 equipment_c_id, material_id, pit_level_material_id, grade, 
                                material_block_name, rehandle, sulphu, block_names_record_id, material_class_id
                                  from productive_routes
                                  where blast_location_id = ? and destination_location_id = ? and
                                  initial_date<=?
                                  and (end_date >? or end_date is null) AND is_active=1
                                  order by initial_date desc;'''
             
            cursor.execute(query_mat_car,origen_original,destino_subido, ts, ts)
            result_Car_Mar = cursor.fetchall()
            if(len(result_Car_Mar)==0):
              #------------------ASIGNACION DE PALA DESPUES DE ESTIMADORES Y NO ENCONTRAR LA POSTURA CORRECTA------------------
              #CHANGE QUERY LISTO?
              query_mat_car2 = '''SELECT TOP 1 equipment_c_id
                                    from productive_routes
                                    where blast_location_id = ? AND
                                    initial_date<=?
                                    AND (end_date >? OR end_date is null) and is_active=1
                                    order by initial_date desc;'''

              cursor.execute(query_mat_car2,origen_original, ts, ts)
              result_Car_Mar2 = cursor.fetchall()
              if(len(result_Car_Mar2)==0):
                cargador_original = 90909
                material_original = 4
                material_block_original = 1
                material_subido = material_original
                cargador_subido = cargador_original
                material_block_subido = material_block_original
                block_name_subido = block_name_original
                material_class_id_subido = material_class_id_original
                flag_sin_pala_material = 1
                print('Sin postura')
                if(flag_estimador_t==1):
                  error_type_cases = 203
              #------------------No se encontro la postura ------------------
              else:
                if(flag_estimador_t==1):
                  error_type_cases = 202
                else:
                   error_type_cases = 205
                cargador_subido = result_Car_Mar2[0][0]
                print('Sin postura, pero con camion y sin material')
            else:
                #Se encontro la postura
                error_type_cases = 201
                cargador_subido = result_Car_Mar[0][0]
                material_subido = result_Car_Mar[0][1]
                material_block_subido = result_Car_Mar[0][2]
                gradeAux = result_Car_Mar[0][3]
                materialBlockAux = result_Car_Mar[0][4]
                rehandleAux = result_Car_Mar[0][5]
                gradeAuxSul = result_Car_Mar[0][6]
                block_name_subido = result_Car_Mar[0][7]
                material_class_id_subido = result_Car_Mar[0][8]
                if(rehandleAux==None):
                    rehandleAux=0
            print(origen_subido, destino_subido, eq_trans,material_subido, cargador_subido)
            print(origen_original, destino_original, eq_trans,material_original, cargador_original)
            print('\n')
            try:
              #CHANGE QUERY LISTO?
              cursor.execute(query_insert_rv, 
                                id, destino_subido,origen_original, trayecto_original,cua, operator_id, cargador_subido, eq_trans, 
                                material_subido, tst,td,ta, tpd,tpa,tsas,tpsas,ts,tps,tt,it,tur,ton, categoria_origen_original,
                                categoria_destino_original, material_block_subido, gradeAux, materialBlockAux, rehandleAux, gradeAuxSul,
                                time_origin_start, inside_time_origin, travelDistance, block_name_subido, reason_id, created_at_simberi, is_reported, material_class_id_subido,
                                ori_start_date_backing, ori_end_date_backing, des_start_date_backing, des_end_date_backing,
                                ori_start_date_tipping, ori_end_date_tipping, des_start_date_tipping, des_end_date_tipping
                                )
              conexion.commit()
              new_reg_load += 1
              try:
                #CHANGE QUERY LISTO?
                #AQUI FALTA UNA VARIBLE DESPUES DE BLOCK_NAME_ORIGINAL
                cursor.execute(query_pala_update,id, destino_original,origen_original, cargador_original, eq_trans,material_original,error_type_cases,trayecto_original,now,None,None,
                                'estimador',None,None,None,None,None,material_block_original,None,rehandleAux, None, block_name_original, None, material_class_id_original, None
                                )
                conexion.commit()
                if(destino_original!=destino_subido):
                    #CHANGE QUERY LISTO?
                    queryDES = '''UPDATE obs_travels set  
                                new_destination_id=? where travel_id=? and updated_by='estimador';'''
                    cursor.execute(queryDES, destino_subido, id)

                if(cargador_original!=cargador_subido or cargador_subido==90909):
                    #CHANGE QUERY LISTO?
                    queryCAR = '''UPDATE obs_travels set  
                                new_equipment_c_id=? where travel_id=? and updated_by='estimador';'''
                    cursor.execute(queryCAR, cargador_subido, id)

                if(material_original!=material_subido or material_subido==4):
                    #CHANGE QUERY LISTO?
                    queryMAT = '''UPDATE obs_travels set new_material_id=?, new_pit_material_level=?, new_block_names_record_id=?, material_class_id_new=?  
                                where travel_id=? and updated_by='estimador';'''
                    cursor.execute(queryMAT, material_subido,material_block_subido, block_name_subido, material_class_id_subido, id)
                conexion.commit()
              except Exception as err:
                print('Error auditoria 1')
                print(err)
                pass            
            except Exception as err:
              print('Error al subir viaje TBC')
              print(err)
              pass
            contadorAobservar +=1
        else:
          #Cuando el viaje es vacio y otro
          #------------------SE INSERTAN LOS VIAJES VACIOS Y OTROS------------------
          try:
            #CHANGE QUERY LISTO?
            
            cursor.execute(query_insert_rv, 
                              id, destino_subido,origen_original, trayecto_original,cua, operator_id, 90909, eq_trans, 
                              4, tst,td,ta, tpd,tpa,tsas,tpsas,ts,tps,tt,it,tur,ton, categoria_origen_original, categoria_destino_original,1, None,None,0,None,
                              time_origin_start, inside_time_origin, travelDistance, block_name_subido, reason_id, created_at_simberi, is_reported, material_class_id_subido,
                              ori_start_date_backing, ori_end_date_backing, des_start_date_backing, des_end_date_backing,
                                ori_start_date_tipping, ori_end_date_tipping, des_start_date_tipping, des_end_date_tipping
                              )

            conexion.commit()
            new_reg_load += 1
          except Exception as err:
            print('Error al subir viaje vacio')
            print(err)
            pass
          #print('Vacio u otro, no se debe ocupar el estimador')
          if(trayecto_original==2):
            contadorVacio +=1
          elif(trayecto_original==4):
            contadorOtro +=1
        
    except Exception as err:
        print(err)
        print('error nonetype peru')
        print("Ocurrió un error:")
        print(f"Tipo de error: {type(err).__name__}")
        print(f"Mensaje de error: {err}")
        print("Rastreo de la pila (stack trace):")
        traceback.print_exc()
        pass
  print('Viajes cargados: ' + str(contadorCorrectos))
  print('Viajes cargados con problemas: ' + str(contadorIncorrectos))
  print('viajes vacios: ' + str(contadorVacio))
  print('viajes TBC: ' + str(contadorAobservar))
  print('viajes otros: ' + str(contadorOtro))
  print(new_reg_load, ' Registers Writed', end='\r', flush=True)
  now_datetime = datetime.datetime.utcnow()
  simberi_datetime = now_datetime  + datetime.timedelta(hours=gmt_hour_zone)
  simberi_time = simberi_datetime.strftime("%H:%M:%S")
  message_output_windows = str(new_reg_load) + ' cycles registers writed (' +simberi_time + ')'
  window['-Output_text-'].update(values['-Output_text-'] + '\n' + message_output_windows)
  #print(deleted_to_improve, ' Registers Deleted')
  print('\nDataframe of Cycles of Load was writed to Database\n')

logo_base64 = (b'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')
icon_base64 = (b'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')
truck_base64 = (b'iVBORw0KGgoAAAANSUhEUgAAAHsAAABSCAMAAABDuluzAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAL9UExURQAAAP///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////4bU3HAAAAD+dFJOUwABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0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')

sg.theme('dark teal 10')

Connect_column = [

    [sg.Text('Connection Settings', font=("Mensura 6", 12))],
    [sg.HorizontalSeparator()],
    [sg.Text("Hostname", size=(10, 1), font=("Mensura 3", 10)), sg.In(
        "199.19.74.79", size=(19, 1), enable_events=True, key="-HOSTNAME-", font=("Mensura 3", 10))],
    [sg.Text("Username", size=(10, 1), font=("Mensura 3", 10)), sg.In(
        "sa", size=(19, 1), enable_events=True, key="-USERNAME-", font=("Mensura 3", 10))],
    [sg.Text("Password ", size=(10, 1), font=("Mensura 3", 10)), sg.In(
        "Model+Mining+Simplefms", size=(19, 1), enable_events=True, key="-PASSWORD-", password_char='•', font=("Mensura 3", 10))],
    [sg.Text("Database ", size=(10, 1), font=("Mensura 3", 10)), sg.In(
        "simplefms_lisbon_mine", size=(19, 1), enable_events=True, key="-DATABASE-", font=("Mensura 3", 10))],

    [sg.Text('', font=("Mensura 3", 10))],
    [sg.Text('Api GPS Settings', font=("Mensura 6", 12))],
    [sg.HorizontalSeparator()],
    [sg.Text("Aplication Id", size=(10, 1), font=("Mensura 3", 10)), sg.In(
        "21", size=(19, 1), enable_events=True, key="-APLICATION_ID-", font=("Mensura 3", 10))],
    [sg.Text('Token Id', size=(10,1), font=("Mensura 3", 10)), sg.In("OQ%2fFQnZx6u6sXu42wCUUknSl19cOtHJknBJw8m6e89vYMr2su0pCOrEtUbrCDTvB", size=(19,1),enable_events=True, key="-TOKEN_GPS_ID-", font=("Mensura 3", 10))],
    [sg.Text('Tag Truck Id', size=(10,1), font=("Mensura 3", 10)), sg.In("367", size=(19,1),enable_events=True, key="-TAG_TRUCK_GPS_ID-", font=("Mensura 3", 10))],
     [sg.Text('Tag Shovel Id', size=(10,1), font=("Mensura 3", 10)), sg.In("361", size=(19,1),enable_events=True, key="-TAG_SHOVEL_GPS_ID-", font=("Mensura 3", 10))],
    [sg.Text('Report Id', size=(10,1), font=("Mensura 3", 10)), sg.In("414", size=(19,1),enable_events=True, key="-REPORT_GPS_ID-", font=("Mensura 3", 10))],
    [sg.Text('Event Rule Id', size=(10,1), font=("Mensura 3", 10)), sg.In("710", size=(19,1),enable_events=True, key="-EVENT_RULE_GPS_ID-", font=("Mensura 3", 10))],
    [sg.Text('GMT Zone', size=(10,1), font=("Mensura 3", 10)), sg.In("-7", size=(19,1),enable_events=True, key="-GMT_HOUR-", font=("Mensura 3", 10))],
    [sg.Text('Hour Prod', size=(10,1), font=("Mensura 3", 10)), sg.In("6", size=(19,1),enable_events=True, key="-HOUR_PRODUCTION-", font=("Mensura 3", 10))],
]

loc_base64 = (b'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')
load_base64 = (b'iVBORw0KGgoAAAANSUhEUgAAACYAAAAmCAMAAACf4xmcAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAALQUExURQAAAAAAAP///4CAgKqqqr+/v////9XV1ba2ttvb27+/v9/f38bGxuPj4+bm5tHR0dXV1djY2MzMzM/Pz9/f38PDw9LS0tXV1c7OztHR0d/f39bW1srKytPT09zc3NXV1c/Pz8vLy9XV1c/Pz9bW1tLS0tPT087OztDQ0NXV1dbW1tPT087OztnZ2c/Pz9bW1s7OztPT09DQ0NXV1dHR0dXV1dHR0dXV1dLS0tbW1tLS0tfX19PT09DQ0NXV1dHR0dbW1tPT09TU1NLS0tXV1c/Pz9PT09DQ0NTU1NHR0dDQ0NPT09TU1NHR0dTU1NPT09bW1tTU1NLS0tXV1dXV1dXV1dHR0c/Pz9TU1NXV1dPT09HR0dTU1NXV1dPT09PT09TU1NLS0tLS0tPT09LS0tLS0tPT09PT09PT09LS0tPT09PT09LS0tLS0tLS0tPT09TU1NPT09LS0tPT09TU1NPT09PT09PT09DQ0NPT09LS0tTU1NPT09PT09HR0dPT09TU1NPT09PT09LS0tLS0tTU1NHR0djY2NHR0dLS0tLS0tLS0tPT09PT09PT09LS0tPT09LS0tXV1dPT09PT09fX19PT09LS0tTU1NTU1NbW1tLS0tPT09LS0tbW1tLS0tLS0tTU1NPT09PT09LS0tPT09LS0tnZ2dLS0tPT09PT09PT09LS0tLS0tTU1NHR0dPT09PT09PT09jY2NXV1dPT09LS0tPT09XV1dbW1tLS0tPT09LS0tPT09XV1dPT09TU1NbW1tbW1tXV1dLS0tPT09LS0tPT09LS0tPT09XV1dnZ2dTU1NPT09fX19PT09PT09XV1dfX19TU1NXV1dfX19PT093d3dPT09TU1NbW1tPT09nZ2dra2tPT09TU1NXV1dbW1tfX19jY2NnZ2dra2tvb29zc3N3d3d7e3t/f3+Dg4OHh4eLi4uTk5OXl5WU7584AAADedFJOUwABAQIDBAQGBwcICAkJCgsMDQ8QEBEREhUWGBkdHR0eICIkJSUoKSorKywuLy8wMjQ0NjY4PD09Pj4/P0BBQkNFRkdJSUtLTE1OUlZYWVldXV9gYGJmamtrbG1wcXN0dXx9fn+BgoSFhoiKjI2Oj5GUlZiYmpucnZ6en5+hpKaoq6yur7CwsrW2ubzAwMPExcfIyMnKy8zNzs/P0dHS0tPU1NXW19fY2Nna29zd3t/g4OLj4+Tn6Ojo6Onp6urq6+zs7e7v7/Dw8vL09PX29vf6+vr7+/v8/P39/f7+/qoH/9UAAAAJcEhZcwAADsMAAA7DAcdvqGQAAAItSURBVDhPYxhOwLfLnw3KxAM0s7ILVaFsPECjuqkkQx7KwQnYfeprilfUSkG5uIBi2/LK3tsPiiWgfBzA9eC7lrQ9T+/nCkEFsALOpNengxhSTj25l8wPFcIGlLrf9qszCFVcfPQghhcqhgW4nbuezsjAINNx6+G9UG6oIAbgSn233gnE0J324P6NQCtTabAwOlCZ+KZeFMyyX3f//uNXT8vBHDTAZH7sSASUHXDs/r17D6ZAeShAMCQvTAvKZmi9DVQ2AcpBAcYLCjKhTAaGXSDTgMqYWVlZWZiggmCgvbQnDspkYNgOUjaZgTv0wMatPSZQQTDgjq6GsoAApOzetaPHz9+7fe/GmbX6UGEgYAk/W+bBB6RBdoCV3bt/H0K98oQoAQGh/Gfn9y6tcq5zAHL2g+Vh4K0XRAkISNY/vf/wwfnNSw2BnJnPnyLAy4fWECUgIFL0Aqjx/vt8ASBHOTwDDjIT7JBijtX70fOHD9/MMwJxonrmL5g+fcaCOdOnz1k0tVQBrAICxOMnnTze7s4BZLpserFxycunDxbuefXi6JzLd7KFIUrAgMfUz0cHHJYN91e7OU262WITufNQgkXV1d1qYAXoYOaDTgaGggvxDHJz1hgwBJ94hhRwSKDxwWpnx8k3my3CdhxONCu/tA+7aW5bXm5Y/OrZg7nbXr08OvvK3RwxqAQaiO1bsXL+/EWrls2fv3TVrBpkn6IAWUsrGLDVA6b8YQEYGABRx/CV1iRFhQAAAABJRU5ErkJggg==')
dump_base64 = (b'iVBORw0KGgoAAAANSUhEUgAAACkAAAAqCAYAAAAu9HJYAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAAFxEAABcRAcom8z8AAASMSURBVFhH7ZdbaFxVFIZTW1FK1UqDVrEPrRSkYtWESr1RQVCkVh+MoE8qShH7oGKF1gdDCzVWaC2+iJWKSkPLKFIU1DzoCFU0MTUhM7nMZJJJJplME4sGJ16atDl+/2btYTKpYMYWDnJ++Nl7r7322v9Z+3LOqYkQIUKE6hAEwSJ4kTXDB4nr7e19tqen53EzhQv5fH4pAnfBJniZmcODZDK5MpVKfdzX1/eamcIHxO0pFAoBQnebKXxA5Fujo6MS+T1ZXWbmcAGRh4aGhgLKaQ5NnZnDg87Ozqs4KK0DAwPB4OBgQP0V6woHEHQTS9wmgWTRiaSMNzY2huOeROAWBOay2awTKKbTaZW/Sry5XXhkMpm1XV1dV6ve39+/ChHr2tvbaxHxEn1Fn0FPfJxQ9uVmF6AM9Nd2d3ffTf8m/O6hfSf1y627aiwi0HcISsKD1PPwBALiTHLGslYioiXwLPXDejVajBKIsZu+9+DT1LdSvspKNP2nVyhBVsNsLpcLyNhZAp+gnVC2CD5HoDKKLY9Pg8TAegtTAg/3Dvb7relAey/ZXWvNhYMA15KZIUT8AVtoZySwXJyoq4e+DuiEIXYNPNDR0bHcBTJg+5A79BZrOhD/EexPWnPhYO9djIiP4JdwolIgwQM7NJ/qgWyYA7YXsB1nP1+pNmKuo/0tbEgkEnfQfxe2jWT3GeqxeDy+xA2sBgTQvpkpFyhxTKDszVB/k/ol5l4Ck1+DbxoeUB3fFu1Zjfelj0XfLPUdNnThIMBDXpSCa2lp+0MTJ1NrKFdrX5WTbK3Cvg3f34mR8beAYvk97anVIHYbftV9OXHylhD8M3v6FDxEMB2O55ngNO0z1GfhDPyzglP4zFZmToJ16dthcyW+1YsU2OzrWLJN2l/+aqH9BEHH4VfwC+iypQxrYk8vTrQMTuPbDj+Hx2n/Njw8LJGt5/3DhEkfhm8TXIehjSU+SPkutulyccqYxNmeLuCzHx6DCdpf47uPMiHxPPjtFv78AFGv23JNMskp+7A4Rv0DfbJp7+p+5Wvd7WX6/jJB6O3X9hgj66fhJPW9lKewP2rh5yMWiy1m8AM474Pvw6OwiWVw10clEHgjAbUff4I3IGoFAo5KDOV2bM8hunlkZKTohSKiDfsRe5jtXDmXYnsQ/yKxWhkbg4/ZFHOhgyFxOM9obygDPgvYXza3OSD4Fk3M8jSaSXv0Prsz95jJfYBgb4YxuBMx39D/i7/k9TrE1oJNh+wpyvVuYCXo3KXgWjoGlCgbwpNk6Vb61pe/PXjievkTtIXyCtmIs18PSd9W53QO4P+Jso1vg9rj4+M3T0xMFJhrwF/684BzPQNPwiLU3iqRySfJZpGJs4jNYTtCBt0Xi94OtA/b70KaLHVZFrX8tS74OUDfvYicIvYUq/Ujq3ESaNyL5jIfBNdbYQNlXSUJUkeQOgJukA9ibuOhSvcYPiuwv4FtBP5MvZkL/Hrr/kcwbjMxfxgbG5tkf6Zob2PZF1v3hQHv96Us2zJ/f/4b6ByQ+eWszLzXaYQIESJE+N+gpuZvNd14obLbfukAAAAASUVORK5CYII=')

numbers_list = []
for i in range(1,61):
  numbers_list.append(str(i))

numbers_list_hour = []
for i in range(1,500):
  numbers_list_hour.append(str(i))

number_list_limit_report = []
for i in range(0,23):
   number_list_limit_report.append(str(i))
number_list_limit_report_lag = []
for i in range(0,10):
   number_list_limit_report_lag.append(str(i))
Test_column = [
    [sg.Text('Options', font=("Mensura 6", 12))],
    [sg.HorizontalSeparator()],
    [sg.Text("Automatic Request", font=("Mensura 6", 11))],
    [sg.CalendarButton('Limit time', size=(10, 1), close_when_date_chosen=True,  format='%Y-%m-%dT%H:%M:%S',target='-limit_time_stamp_reports-', location=(0, 0), no_titlebar=False, font=("Mensura 3", 10))
                       , sg.Input("2024-06-14T05:00:00", key='-limit_time_stamp_reports-', size=(18, 2), font=("Mensura 3", 10))],
    [sg.Text("Read Frequency:      ", font=("Mensura 3", 10)), sg.Combo(numbers_list, default_value='2', key='-ComboFreq-', font=("Mensura 3", 10), size=(10, 10), enable_events=True)],
    [sg.Text("Hours Reports:         ", font=("Mensura 3", 10)), sg.Combo(numbers_list_hour, default_value='5', key='-ComboHours-', font=("Mensura 3", 10), size=(10, 10), enable_events=True)],
    #[sg.Text("Limit Hour Reported: ", font=("Mensura 3", 10)), sg.Combo(number_list_limit_report, default_value='7', key='-ComboHoursLimit-', font=("Mensura 3", 10), size=(10, 10), enable_events=True)],
    #[sg.Text("Hours Reports Lag:   ", font=("Mensura 3", 10)), sg.Combo(number_list_limit_report_lag, default_value='1', key='-ComboHoursLag-', font=("Mensura 3", 10), size=(10, 10), enable_events=True)],
    [sg.Button("Activate", size=(30, 2), disabled=False,
               key='-btnAuto-', font=("Mensura 3", 10))],

    [sg.Text('', font=("Mensura 3", 10))],

]

Query_column = [
    [sg.Text('Output', font=("Mensura 6", 12))],
    [sg.HorizontalSeparator()],
    [sg.Multiline(size=(40, 20), font=("Mensura 3", 10), key='-Output_text-',
                  background_color='gray24', text_color='white', autoscroll=True)],
]

Banner_column = [

]

logo_base64 = (b'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')
icon_base64 = (b'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')

layout = [
    [sg.Button('', image_data=logo_base64,
               button_color=(sg.theme_background_color(),
                             sg.theme_background_color()),
               border_width=0, key='JustImage'),
     sg.VerticalSeparator(color='turquoise4'),
     sg.Text("Gateway-Lisbon Mine-Demo", font=("Mensura 6", 35, 'bold'), size=(25, 1)), ],
    [sg.HorizontalSeparator(color='turquoise4')],
    [sg.Column(Connect_column, vertical_alignment='top'), sg.VerticalSeparator(color='turquoise4'), sg.Column(Test_column, vertical_alignment='top'), sg.VerticalSeparator(color='turquoise4'), sg.Column(Query_column, vertical_alignment='top', element_justification='left')]
]

window = sg.Window("Gateway API-Database", layout, margins=(
    5, 6), location=(0, 0), size=(900, 600), icon=icon_base64)

op_code = 445

# Create an event loop

Auto_Request = False
minuts_count = 0
minuteCountThirty = 0
hourCountDistance = 0
ticks_count = 0
ticks_count_4x4 = 0


current_time = datetime.datetime.now()
delta_time = datetime.datetime.now() - datetime.timedelta(seconds=43200)
print("Current Time =", current_time)
print("Delta Time =", delta_time)

while True:
    try:
        try:
            event, values = window.read(timeout=200)
            # End program if user closes window or
            # presses the OK button
        except Exception as err:
            print(err)
            error_log = str(traceback.print_exc())
            Alternative_function.create_log_file('errores_log.log', error_log)
            Alternative_function.create_log_file('errores_log2.log', err)
            #Alternative_function.telegramBot(str('Conflictos en la aplicación Gateway SIMBERI Beacon corriendo en el Servidor ' + socket.gethostbyname(socket.gethostname()) ))


        if Auto_Request:
            ticks_count += 1

            if ticks_count == 300: #1 minuto
              ticks_count = 0
              minuts_count += 1
              minuteCountThirty += 1
              hourCountDistance += 1
              
              hostname = values['-HOSTNAME-']
              username = values['-USERNAME-']
              password = values['-PASSWORD-']
              database = values['-DATABASE-']
              segundos_combo_box = int(values['-ComboHours-']) * 3600

              aplicaction_id = values['-APLICATION_ID-']
              token_id = values['-TOKEN_GPS_ID-']
              event_rule_id = values['-EVENT_RULE_GPS_ID-']
              tag_truck_id = values['-TAG_TRUCK_GPS_ID-']
              tag_shovel_id = values['-TAG_SHOVEL_GPS_ID-']
              report_id = values['-REPORT_GPS_ID-']
              gmt_zone_hour = int(values['-GMT_HOUR-'])
              mine_hour_production = int(values['-HOUR_PRODUCTION-'])

              print(aplicaction_id)
              print(token_id)
              print(tag_truck_id)
              print(report_id)
              print(gmt_zone_hour)
              #VARIABLE IMPORTANTE NUEVA LOGICA
              if(first_join==True):
                  print('Primer ingreso se toma en cuenta el limit_time_stamp_report')
                  limit_time_stamp_aux = values['-limit_time_stamp_reports-']
                  limit_time_stamp = datetime.datetime.strptime(limit_time_stamp_aux, "%Y-%m-%dT%H:%M:%S")
                  limit_is_reported_time_stamp = limit_time_stamp
                  first_join=False
              print('---------------------------------------')
              print('Limite de tiempo aplicado a produccion')
              print(type(limit_time_stamp))
              print(limit_time_stamp)
              print(limit_is_reported_time_stamp)
              print('---------------------------------------')
              connect_sqlServer =[hostname, username, password, database]
              engine_string = 'postgresql://'+ username +':' + password + '@' + hostname + '/' + database
              end_date = datetime.date.today() + datetime.timedelta(days=1)
              start_date = end_date + datetime.timedelta(days=-3)
              try:
                Get_API_Data.ultimo_registro(connect_sqlServer, aplicaction_id, tag_truck_id, tag_shovel_id, token_id)
              except Exception as err:
                print('Fallo la recoleccion del ultimo registro')
              Get_API_Data.llenado_Tipos_Zonas(connect_sqlServer, aplicaction_id, token_id)
              Get_API_Data.eliminarZonas(connect_sqlServer, aplicaction_id, token_id)
              Get_API_Data.cargarZonas(connect_sqlServer, aplicaction_id, token_id)
              try:
                SummaryProductionByWebPage.MainReconciliationSummaryTravelsActions(connect_sqlServer)
              except Exception as err:
                print(err)
              #Alternative_function.main_upload_shovel_status(connect_sqlServer)
              try:
                #AQUI VA EL MANEJO DE EVENTOS.
                #manejoEventos()
                print('a')
              except Exception as err:
                print(err)  
              if minuts_count >= int(values['-ComboFreq-']):
                
                shift_ending = False
                segundos_reportes = segundos_combo_box
                print('El reporte debe leer los ultimos segundos: ', str(segundos_reportes))
                minuts_count = 0
                current_hour = datetime.datetime.now().hour
                current_minut = datetime.datetime.now().minute
                 
                if (current_hour == 4 and current_minut <10):
                  shift_ending = False
                  segundos_reportes = 86400
                  minuteCountThirty = 0
                  hourCountDistance = 0
                  print('Fin del turno')
                  #Alternative_function.telegramBot(str('Cambio el turno en Simberi se solicita reporte de 24 h ' + socket.gethostbyname(socket.gethostname()) ))
                """"
                if (current_hour == 16 and current_minut <10):
                  segundos_reportes = 86400
                  minuteCountThirty = 0
                  hourCountDistance = 0
                  print ('Termino dia')
                  Alternative_function.cleanProductionTravelsSummary(connect_sqlServer)
                  #Alternative_function.telegramBot(str('Cambio el dia en Simberi se solicita reporte de 24 h' + socket.gethostbyname(socket.gethostname()) ))
                else:
                    print('Se lee el reporte con un tramo de 12 horas ')

                #Se debe cambiar a las 18 actualmente esta en 19.    
                if (current_hour == 18 and current_minut <30):
                   #cambia el estado a is_reported=0 despues de las 7 de la mañana
                   #print('Se debe hacer el cambio de estado en registros anteriores')
                   #new_filter_reported_datetime = Get_Data_BD.GetActualShiftDate(connect_sqlServer)
                   print('Se activa limite para registros is_reported')
                   #if(new_filter_reported_datetime != None):
                   #   limit_is_reported_time_stamp = new_filter_reported_datetime
                      #Alternative_function.telegramBot('Se aplica filtro is_reported')

                if (current_hour == 21 and current_minut <30):
                  #Bloquea la informacion del turno anterior.
                  #print('Se debe hacer el bloqueo de registros anteriores.')
                  #new_limit_Datetime = Get_Data_BD.GetActualShiftDate(connect_sqlServer)
                  print('Se activa nuevo limite.')
                  #print(new_limit_Datetime)
                  #if(new_limit_Datetime != None):
                  #   limit_time_stamp = new_limit_Datetime
                     #Alternative_function.telegramBot('Corte de turno cambiando con exito')

                """
                delta_time = (datetime.datetime.now() - datetime.timedelta(seconds=segundos_reportes)).strftime('%Y-%m-%dT%H:%M:%SZ')
                print("From =", delta_time)

                
                current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
                print("To =", current_time)
                tiempoFin= datetime.datetime.now() + datetime.timedelta(days=2)
                End_Request = tiempoFin.strftime("%Y-%m-%dT%H:%M:%SZ")
                print("Date prueba", End_Request)

                print("Reading GPSGate API... \n")
                try:
                  end_date = datetime.date.today() + datetime.timedelta(days=1)
                  start_date = end_date + datetime.timedelta(days=-2)
                  fecha_events = str(start_date.strftime("%Y-%m-%d"))
                  Reconciler_Summary_Travels.reconciliationSummaryTravelsActions(connect_sqlServer)


                  #ReconcilerTumRecords.mainReconciliadorTUmByHourAndRip(connect_sqlServer)

                  df_load_cycles_Simberi  = Procesing_Report_Travel.New_Read_API_Reports(delta_time, End_Request, aplicaction_id, report_id, event_rule_id,tag_truck_id, connect_sqlServer, limit_time_stamp, mine_hour_production, token_id)
                  #Get_API_Data.main_read_events(fecha_events, end_date,False, connect_sqlServer, aplicaction_id, tag_truck_id, token_id)
                  
                  print('Termino el procesamiento')
                  window['-Output_text-'].update(values['-Output_text-'] 
                                                  + '\n' + str(len(df_load_cycles_Simberi))  + ' cycles registers readed from API' 
                                                  
                                                  )
                  try:
                    #AlertErrorTUM.MainAlertTum(connect_sqlServer)
                    SummaryProductionByWebPage.MainReconciliationSummaryTravelsActions(connect_sqlServer)
                    
                  except Exception as err:
                     print(err)
    
                except Exception as e:
                  print("No Data")
                  print( str(e) )

                print("Writting Database SQL Server... \n")
                New_Load_to_Database(connect_sqlServer, df_load_cycles_Simberi, limit_is_reported_time_stamp, gmt_zone_hour)
                try:
                  #aqui va el reported
                  print('Funcion Uncheck desactivada')
                  #AutomaticUncheckExtraload.main_reconciliation_load_production(connect_sqlServer)
                except Exception as err:
                   print(err)

              if(hourCountDistance >= 40):
                hourCountDistance = 0
                fecha_hora_actual = datetime.datetime.now()
                messageTumByHour = None
                try:
                  print('Va a reconciliar')
                  Reconciler_Distance_Travels.mainReconcilerDistanceTravels(connect_sqlServer, False)
                except Exception as err:
                  print(err)
                  messageReconcilerDistance = 'Fallo el proceso de reconciliacion de distancias: ' + str(fecha_hora_actual)
                  Alternative_function.create_log_file('log_reconciler_distance.log', messageReconcilerDistance)
        if event == "OK" or event == sg.WIN_CLOSED:
            break
        
        elif event == "-btnAuto-":
            if not Auto_Request:
              Auto_Request = True
              window['-btnAuto-'].update("Deactivate")
            else:
              Auto_Request = False
              window['-btnAuto-'].update("Activate")

        elif event == "btnSave":
            hostname = values['-HOSTNAME-']
            username = values['-USERNAME-']
            password = values['-PASSWORD-']
            database = values['-DATABASE-']


            print("Testing querys SQL Server... \n")
            connect_sqlServer =[hostname, username, password, database]  
            Get_API_Data.llenado_Tipos_Zonas(connect_sqlServer, aplicaction_id, token_id)
            Get_API_Data.eliminarZonas(connect_sqlServer, aplicaction_id, token_id)
            Get_API_Data.cargarZonas(connect_sqlServer, aplicaction_id, token_id)
            Get_API_Data.ultimo_registro(connect_sqlServer, aplicaction_id, tag_truck_id, tag_shovel_id, token_id)
    except Exception as err:
        print(err)
        error_log = str(traceback.print_exc())
        Alternative_function.create_log_file('errores_log.log', error_log)
        Alternative_function.create_log_file('errores_log2.log', err)

        #Alternative_function.telegramBot(str('Conflictos con la aplicación Gateway Beacon, corriendo en el Servidor ' + socket.gethostbyname(socket.gethostname()) ))

window.close()