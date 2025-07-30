import warnings
import pandas as pd
warnings.simplefilter('ignore')
import Get_Data_BD
import Alternative_function

def ThereIsProductiveRoute(sql_connect, origin_group, destination_group, time_stamp):
  """ This function is responsible for determining the existence of a productive route
        Args:
            sqlConnect  : Array []
            origin_group : String
            destination_group : String
            time_stamp : Datetime
        Return: Boolean
  """
  print('Search productive route . . . . .')
  is_productive_route_list = []
  is_productive_route = False
  try:
    is_productive_route_list = Get_Data_BD.GetProductiveRoutesByOriginDestination(sql_connect, origin_group, destination_group, time_stamp, time_stamp)
    if(len(is_productive_route_list)>0):
      print('Existe ruta productiva')
      is_productive_route = True
  except Exception as err:
    print(err)     
  return is_productive_route

def PatternBlastRoardSheetingDump(df_load_cycles, sql_connect):
  """Detects pattern Blast->Inpit->Dump and transforms it into Blast->Dump
  Args:
      df_load_cycles : Dataframe
      sql_connect : array
  Returns: Dataframe
  """
  
  df_load_cycles_post = pd.DataFrame(columns = ['Equipo','Cargador','Blast','Dump','time_depart', 'time_arrive', 'time_production_depart','time_production_arrive','time_spotting_and_spotted','time_production_spotting_and_spotted', 'time_stamp', 'time_production_stamp', 'traveling_time','inside_time'])
  for equipment in df_load_cycles.Equipo.unique():
    df_load_cycles_by_equipment = df_load_cycles[ df_load_cycles['Equipo'] == equipment ].reset_index().drop('index', axis=1)
    for i in range(len(df_load_cycles_by_equipment)):
      #variables para detectar si el registro debe ser agregado o no
      it_be_added = True
      is_deleted_record = False
      actual_origin_group = df_load_cycles_by_equipment.Categoria_origen[i]
      actual_destination_group = df_load_cycles_by_equipment.Categoria_destino[i]
      actual_time_stamp = df_load_cycles_by_equipment.time_stamp[i]
      actual_origin = df_load_cycles_by_equipment.Blast[i]
      actual_destination =  df_load_cycles_by_equipment.Dump[i]
      equipment_name = df_load_cycles_by_equipment.Equipo[i]

      #Variables que permiten asignar datos, cuando se cumple el patron.
      blast_id_upload = df_load_cycles_by_equipment.Blast_id[i]
      blast_upload = df_load_cycles_by_equipment.Blast[i]
      blast_categoria_ori_upload = df_load_cycles_by_equipment.Categoria_origen[i]
      time_depart_upload = df_load_cycles_by_equipment.time_depart[i]
      time_production_dep_upload = df_load_cycles_by_equipment.time_production_depart[i]
      traveling_time_upload = df_load_cycles_by_equipment.traveling_time[i]
      time_origin_start_upload = df_load_cycles_by_equipment.time_origin_start[i]
      inside_time_origin_upload = df_load_cycles_by_equipment.inside_time_origin[i]
      travel_distance_upload = df_load_cycles_by_equipment.travel_distance[i]
      odometer_origin_upload = df_load_cycles_by_equipment.odometer_origin[i]
      travel_type_upload = df_load_cycles_by_equipment.Tipo_trayecto[i] 

      if(i==0):
        print('Es el primero, se debe agregar')
        it_be_added =  True
      elif((i==len(df_load_cycles_by_equipment) - 1) and actual_origin_group=='Blasts' and actual_destination_group == 'Inpits'):
        print('Es el ultimo y debe buscar productive route') 

        Alternative_function.telegramBot('Prueba: Es el ultimo registro del equipo b->i' + str(equipment_name))
        message_ultimo_registro = 'Origen: ' + str(actual_origin) + ' Destination: ' + str(actual_destination)
        Alternative_function.telegramBot(message_ultimo_registro)
        try:
          Alternative_function.telegramBot(actual_time_stamp)
        except Exception as err:
          message_error = 'Fallo el mensaje cuando destino registro es el ultimo y cumple patron B -> I -> D . ' + str(equipment_name)
          Alternative_function.create_log_file("last_record_by_equipment.log", message_error)
          Alternative_function.create_log_file("last_record_by_equipment.log", err)
        is_productive_route = ThereIsProductiveRoute(sql_connect, actual_origin, actual_destination, actual_time_stamp)
        if(is_productive_route == False):
          it_be_added = False
      else:
        last_origin_group = df_load_cycles_by_equipment.Categoria_origen[i-1]
        last_destination_group = df_load_cycles_by_equipment.Categoria_destino[i-1]

        last_origin = df_load_cycles_by_equipment.Blast[i-1]
        last_destination = df_load_cycles_by_equipment.Dump[i-1]
        last_time_stamp = df_load_cycles_by_equipment.time_stamp[i-1]

        if(last_origin_group =='Blasts' and last_destination_group == 'Inpits' and
        actual_origin_group=='Inpits' and (actual_destination_group=='Dumps' or
        actual_destination_group=='Stockpiles' or actual_destination_group=='Crushers')):
          print('Se debe revisar productive route')
          Alternative_function.telegramBot('Se encontro un patron Blast -> Inpits -> Dumps . ' + str(equipment_name))
          is_productive_route_patron = ThereIsProductiveRoute(sql_connect, last_origin, last_destination, last_time_stamp)
          if(is_productive_route_patron == True):
            Alternative_function.telegramBot('Patron tiene productive route')
            message_pr= 'Origen: ' + str(last_origin) + ' Destination: ' + str(last_destination)
            Alternative_function.telegramBot(message_pr)
            try:
              Alternative_function.telegramBot(actual_time_stamp)
            except Exception as err:
              message_error = 'Fallo el mensaje cuando destino registro es el ultimo y cumple patron B -> I -> D . ' + str(equipment_name)
              Alternative_function.create_log_file("patron_b_i_d_pr.log", message_error)
              Alternative_function.create_log_file("patron_b_i_d_pr.log", err)
            it_be_added = True
          else:
            print('Se debe hacer el cambio de variables')
            Alternative_function.telegramBot('Patron B->I->D. No presenta Pr, por ende se debe transfomar el registro')
            message_change1= 'L_Origen: ' + str(df_load_cycles_by_equipment.Blast[i-1]) + 'L_Destination: ' + str(df_load_cycles_by_equipment.Dump[i-1])
            message_change2= 'A_Origen: ' + str(last_origin) + 'A_Destination: ' + str(last_destination)
            Alternative_function.telegramBot(message_change1)
            Alternative_function.telegramBot(message_change2)
            is_deleted_record = True
            it_be_added = True
            #variables a insertar 
            blast_id_upload = df_load_cycles_by_equipment.Blast_id[i-1]
            blast_upload = df_load_cycles_by_equipment.Blast[i-1]
            blast_categoria_ori_upload = df_load_cycles_by_equipment.Categoria_origen[i-1]
            time_depart_upload = df_load_cycles_by_equipment.time_depart[i-1]
            time_production_dep_upload = df_load_cycles_by_equipment.time_production_depart[i-1]
            traveling_time_upload = df_load_cycles_by_equipment.traveling_time[i-1] + df_load_cycles_by_equipment.inside_time[i-1] + df_load_cycles_by_equipment.traveling_time[i]
            time_origin_start_upload = df_load_cycles_by_equipment.time_origin_start[i-1]
            inside_time_origin_upload = df_load_cycles_by_equipment.inside_time_origin[i-1]
            travel_distance_upload = df_load_cycles_by_equipment.odometer_destination[i]  - df_load_cycles_by_equipment.odometer_origin[i-1]
            odometer_origin_upload = df_load_cycles_by_equipment.odometer_origin[i-1]
            travel_type_upload = 1
        else:
          it_be_added = True
            
      if(is_deleted_record == True):
        #Se quita la ultima columna, ya que, tiene el patron Blasts -> Inpits
        df_load_cycles_post.drop(df_load_cycles_post.tail(1).index,inplace=True)
      
      if(it_be_added==True):
        df_load_cycles_post = df_load_cycles_post.append(
            {'Equipo_id':df_load_cycles_by_equipment.Equipo_id[i],
             'Equipo':df_load_cycles_by_equipment.Equipo[i],
             'Blast_id':blast_id_upload,
             'Blast':blast_upload,
             'Dump_id':df_load_cycles_by_equipment.Dump_id[i],
             'Dump':df_load_cycles_by_equipment.Dump[i],
             'Categoria_origen':blast_categoria_ori_upload,
             'Categoria_destino':df_load_cycles_by_equipment.Categoria_destino[i],
             'Tipo_trayecto': travel_type_upload,
             'Tonelaje':df_load_cycles_by_equipment.Tonelaje[i],
             'time_depart':time_depart_upload,
             'time_production_depart':time_production_dep_upload,
             'time_arrive':df_load_cycles_by_equipment.time_arrive[i], 
             'time_production_arrive':df_load_cycles_by_equipment.time_production_arrive[i],
             'time_spotting_and_spotted':df_load_cycles_by_equipment.time_spotting_and_spotted[i],
             'time_production_spotting_and_spotted':df_load_cycles_by_equipment.time_production_spotting_and_spotted[i],
             'time_stamp':df_load_cycles_by_equipment.time_stamp[i],
             'time_production_stamp':df_load_cycles_by_equipment.time_production_stamp[i],
             'traveling_time': traveling_time_upload,
             'inside_time':df_load_cycles_by_equipment.inside_time[i],
             'Turno':df_load_cycles_by_equipment.Turno[i],
             'Cuadrilla':df_load_cycles_by_equipment.Cuadrilla[i],
             'index':str(df_load_cycles_by_equipment.time_depart[i].strftime('%y%m%d%H%M%S')) + str(int(df_load_cycles_by_equipment.Equipo_id[i])),
             'travel_same_type':df_load_cycles_by_equipment.travel_same_type[i],
             'time_origin_start':time_origin_start_upload,
             'inside_time_origin':inside_time_origin_upload,
             'case':  df_load_cycles_by_equipment.case[i],
             'travel_distance': travel_distance_upload,
             'odometer_origin': odometer_origin_upload,
             'odometer_destination': df_load_cycles_by_equipment.odometer_destination[i] 
            }, ignore_index=True)
  return df_load_cycles_post
                     
def patron_blast_blast_dump(df_load_cycles):
  
  counter_elements = 0
  df_load_cycles_post = pd.DataFrame(columns = ['Equipo','Cargador','Blast','Dump','time_depart', 'time_arrive', 'time_production_depart','time_production_arrive','time_spotting_and_spotted','time_production_spotting_and_spotted', 'time_stamp', 'time_production_stamp', 'traveling_time','inside_time'])
  for j in df_load_cycles.Equipo.unique():
    df_load_cycles_1 = df_load_cycles[ df_load_cycles['Equipo'] == j ].reset_index().drop('index', axis=1)
    for i in range(len(df_load_cycles_1)):
      if i > 0:   
        #counter_elements += 1
        #print(counter_elements, ' Travel Registers Processed BBD', end='\r', flush=True)

        if((df_load_cycles_1.Categoria_origen[i-1] == 'Blasts') and 
            df_load_cycles_1.Dump_id[i-1] == 372 and
            df_load_cycles_1.Blast_id[i] == 372):
          print('Patron BBD detectado: ',df_load_cycles_1.Blast[i-1],'->',df_load_cycles_1.Dump[i-1],'->',df_load_cycles_1.Dump[i],'  |', df_load_cycles_1.time_depart[i], df_load_cycles_1.Equipo[i])
          df_load_cycles_post.drop(df_load_cycles_post.tail(1).index,inplace=True) #Drop Last Register
          travel_time = df_load_cycles_1.traveling_time[i-1] + df_load_cycles_1.inside_time[i-1] + df_load_cycles_1.traveling_time[i]
          travelDistance = df_load_cycles_1.odometer_destination[i]  - df_load_cycles_1.odometer_origin[i-1]
          df_load_cycles_post = df_load_cycles_post.append({  'Equipo_id':df_load_cycles_1.Equipo_id[i],
                                                              'Equipo':df_load_cycles_1.Equipo[i],
                                                              'Blast_id':df_load_cycles_1.Blast_id[i-1],
                                                              'Blast':df_load_cycles_1.Blast[i-1],
                                                              'Dump_id':df_load_cycles_1.Dump_id[i],
                                                              'Dump':df_load_cycles_1.Dump[i],
                                                              'Categoria_origen':df_load_cycles_1.Categoria_origen[i-1],
                                                              'Categoria_destino':df_load_cycles_1.Categoria_destino[i],
                                                              'Tipo_trayecto': 1,
                                                              'Tonelaje':df_load_cycles_1.Tonelaje[i],
                                                              'time_depart':df_load_cycles_1.time_depart[i-1],
                                                              'time_production_depart':df_load_cycles_1.time_production_depart[i-1],
                                                              'time_arrive':df_load_cycles_1.time_arrive[i], 
                                                              'time_production_arrive':df_load_cycles_1.time_production_arrive[i],
                                                              'time_spotting_and_spotted':df_load_cycles_1.time_spotting_and_spotted[i],
                                                              'time_production_spotting_and_spotted':df_load_cycles_1.time_production_spotting_and_spotted[i],
                                                              'time_stamp':df_load_cycles_1.time_stamp[i],
                                                              'time_production_stamp':df_load_cycles_1.time_production_stamp[i],
                                                              'traveling_time': travel_time,
                                                              'inside_time':df_load_cycles_1.inside_time[i],
                                                              'Turno':df_load_cycles_1.Turno[i],
                                                              'Cuadrilla':df_load_cycles_1.Cuadrilla[i],
                                                              'index':str(df_load_cycles_1.time_depart[i].strftime('%y%m%d%H%M%S')) + str(int(df_load_cycles_1.Equipo_id[i])),
                                                              'travel_same_type':df_load_cycles_1.travel_same_type[i],
                                                              'time_origin_start':df_load_cycles_1.time_origin_start[i-1],
                                                              'inside_time_origin':df_load_cycles_1.inside_time_origin[i-1],
                                                              'case':  df_load_cycles_1.case[i],
                                                              'travel_distance': travelDistance,
                                                              'odometer_origin': df_load_cycles_1.odometer_origin[i-1],
                                                              'odometer_destination': df_load_cycles_1.odometer_destination[i] 
                                                              }, ignore_index=True)
        else:
          df_load_cycles_post = df_load_cycles_post.append({  'Equipo_id':df_load_cycles_1.Equipo_id[i],
                                                              'Equipo':df_load_cycles_1.Equipo[i],
                                                              'Blast_id':df_load_cycles_1.Blast_id[i],
                                                              'Blast':df_load_cycles_1.Blast[i],
                                                              'Dump_id':df_load_cycles_1.Dump_id[i],
                                                              'Dump':df_load_cycles_1.Dump[i],
                                                              'Categoria_origen':df_load_cycles_1.Categoria_origen[i],
                                                              'Categoria_destino':df_load_cycles_1.Categoria_destino[i],
                                                              'Tipo_trayecto': df_load_cycles_1.Tipo_trayecto[i],
                                                              'Tonelaje':df_load_cycles_1.Tonelaje[i],
                                                              'time_depart':df_load_cycles_1.time_depart[i],
                                                              'time_production_depart':df_load_cycles_1.time_production_depart[i],
                                                              'time_arrive':df_load_cycles_1.time_arrive[i], 
                                                              'time_production_arrive':df_load_cycles_1.time_production_arrive[i],
                                                              'time_spotting_and_spotted':df_load_cycles_1.time_spotting_and_spotted[i],
                                                              'time_production_spotting_and_spotted':df_load_cycles_1.time_production_spotting_and_spotted[i],
                                                              'time_stamp':df_load_cycles_1.time_stamp[i],
                                                              'time_production_stamp':df_load_cycles_1.time_production_stamp[i],
                                                              'traveling_time': df_load_cycles_1.traveling_time[i],
                                                              'inside_time':df_load_cycles_1.inside_time[i],
                                                              'Turno':df_load_cycles_1.Turno[i],
                                                              'Cuadrilla':df_load_cycles_1.Cuadrilla[i],
                                                              'index':str(df_load_cycles_1.time_depart[i].strftime('%y%m%d%H%M%S')) + str(int(df_load_cycles_1.Equipo_id[i])),
                                                              'travel_same_type':df_load_cycles_1.travel_same_type[i],
                                                              'time_origin_start':df_load_cycles_1.time_origin_start[i],
                                                              'inside_time_origin':df_load_cycles_1.inside_time_origin[i],
                                                              'case':  df_load_cycles_1.case[i],
                                                              'travel_distance': df_load_cycles_1.travel_distance[i],
                                                              'odometer_origin': df_load_cycles_1.odometer_origin[i],
                                                              'odometer_destination': df_load_cycles_1.odometer_destination[i]  
                                                              }, ignore_index=True) 
      else:
        df_load_cycles_post = df_load_cycles_post.append({  'Equipo_id':df_load_cycles_1.Equipo_id[i],
                                                              'Equipo':df_load_cycles_1.Equipo[i],
                                                              'Blast_id':df_load_cycles_1.Blast_id[i],
                                                              'Blast':df_load_cycles_1.Blast[i],
                                                              'Dump_id':df_load_cycles_1.Dump_id[i],
                                                              'Dump':df_load_cycles_1.Dump[i],
                                                              'Categoria_origen':df_load_cycles_1.Categoria_origen[i],
                                                              'Categoria_destino':df_load_cycles_1.Categoria_destino[i],
                                                              'Tipo_trayecto': df_load_cycles_1.Tipo_trayecto[i],
                                                              'Tonelaje':df_load_cycles_1.Tonelaje[i],
                                                              'time_depart':df_load_cycles_1.time_depart[i],
                                                              'time_production_depart':df_load_cycles_1.time_production_depart[i],
                                                              'time_arrive':df_load_cycles_1.time_arrive[i], 
                                                              'time_production_arrive':df_load_cycles_1.time_production_arrive[i],
                                                              'time_spotting_and_spotted':df_load_cycles_1.time_spotting_and_spotted[i],
                                                              'time_production_spotting_and_spotted':df_load_cycles_1.time_production_spotting_and_spotted[i],
                                                              'time_stamp':df_load_cycles_1.time_stamp[i],
                                                              'time_production_stamp':df_load_cycles_1.time_production_stamp[i],
                                                              'traveling_time': df_load_cycles_1.traveling_time[i],
                                                              'inside_time':df_load_cycles_1.inside_time[i],
                                                              'Turno':df_load_cycles_1.Turno[i],
                                                              'Cuadrilla':df_load_cycles_1.Cuadrilla[i],
                                                              'index':str(df_load_cycles_1.time_depart[i].strftime('%y%m%d%H%M%S')) + str(int(df_load_cycles_1.Equipo_id[i])),
                                                              'travel_same_type':df_load_cycles_1.travel_same_type[i],
                                                              'time_origin_start':df_load_cycles_1.time_origin_start[i],
                                                              'inside_time_origin':df_load_cycles_1.inside_time_origin[i],
                                                              'case':  df_load_cycles_1.case[i],
                                                              'travel_distance': df_load_cycles_1.travel_distance[i],
                                                              'odometer_origin': df_load_cycles_1.odometer_origin[i],
                                                              'odometer_destination': df_load_cycles_1.odometer_destination[i] 
                                                              }, ignore_index=True)                                                                                                              
  return df_load_cycles_post

def patron_385(df_load_cycles):
  counter_elements = 0
  df_load_cycles_post = pd.DataFrame(columns = ['Equipo','Cargador','Blast','Dump','time_depart', 'time_arrive', 'time_production_depart','time_production_arrive','time_spotting_and_spotted','time_production_spotting_and_spotted', 'time_stamp', 'time_production_stamp', 'traveling_time','inside_time'])
  for j in df_load_cycles.Equipo.unique():
    df_load_cycles_1 = df_load_cycles[ df_load_cycles['Equipo'] == j ].reset_index().drop('index', axis=1)
    for i in range(len(df_load_cycles_1)):
      if i > 0:   
        #counter_elements += 1
        #print(counter_elements, ' Travel Registers Processed patron_385', end='\r', flush=True)

        if((df_load_cycles_1.Categoria_origen[i-1] == 'Blasts' or df_load_cycles_1.Categoria_origen[i-1] == 'Stockpiles') and 
            df_load_cycles_1.Dump_id[i-1] == 385 and
            df_load_cycles_1.Blast_id[i] == 385) and  df_load_cycles_1.Categoria_destino[i]!='Shop_Zone':
          print('Patron 385 detectado: ',df_load_cycles_1.Blast[i-1],'->',df_load_cycles_1.Dump[i-1],'->',df_load_cycles_1.Dump[i],'  |', df_load_cycles_1.time_depart[i], df_load_cycles_1.Equipo[i])
          df_load_cycles_post.drop(df_load_cycles_post.tail(1).index,inplace=True) #Drop Last Register
          travel_time = df_load_cycles_1.traveling_time[i-1] + df_load_cycles_1.inside_time[i-1] + df_load_cycles_1.traveling_time[i]
          travelDistance = df_load_cycles_1.odometer_destination[i]  - df_load_cycles_1.odometer_origin[i-1]
          df_load_cycles_post = df_load_cycles_post.append({  'Equipo_id':df_load_cycles_1.Equipo_id[i],
                                                              'Equipo':df_load_cycles_1.Equipo[i],
                                                              'Blast_id':df_load_cycles_1.Blast_id[i-1],
                                                              'Blast':df_load_cycles_1.Blast[i-1],
                                                              'Dump_id':df_load_cycles_1.Dump_id[i],
                                                              'Dump':df_load_cycles_1.Dump[i],
                                                              'Categoria_origen':df_load_cycles_1.Categoria_origen[i-1],
                                                              'Categoria_destino':df_load_cycles_1.Categoria_destino[i],
                                                              'Tipo_trayecto': 1,
                                                              'Tonelaje':df_load_cycles_1.Tonelaje[i],
                                                              'time_depart':df_load_cycles_1.time_depart[i-1],
                                                              'time_production_depart':df_load_cycles_1.time_production_depart[i-1],
                                                              'time_arrive':df_load_cycles_1.time_arrive[i], 
                                                              'time_production_arrive':df_load_cycles_1.time_production_arrive[i],
                                                              'time_spotting_and_spotted':df_load_cycles_1.time_spotting_and_spotted[i],
                                                              'time_production_spotting_and_spotted':df_load_cycles_1.time_production_spotting_and_spotted[i],
                                                              'time_stamp':df_load_cycles_1.time_stamp[i],
                                                              'time_production_stamp':df_load_cycles_1.time_production_stamp[i],
                                                              'traveling_time': travel_time,
                                                              'inside_time':df_load_cycles_1.inside_time[i],
                                                              'Turno':df_load_cycles_1.Turno[i],
                                                              'Cuadrilla':df_load_cycles_1.Cuadrilla[i],
                                                              'index':str(df_load_cycles_1.time_depart[i].strftime('%y%m%d%H%M%S')) + str(int(df_load_cycles_1.Equipo_id[i])),
                                                              'travel_same_type':df_load_cycles_1.travel_same_type[i],
                                                              'time_origin_start':df_load_cycles_1.time_origin_start[i-1],
                                                              'inside_time_origin':df_load_cycles_1.inside_time_origin[i-1],
                                                              'case':  df_load_cycles_1.case[i],
                                                              'travel_distance': travelDistance,
                                                              'odometer_origin': df_load_cycles_1.odometer_origin[i-1],
                                                              'odometer_destination': df_load_cycles_1.odometer_destination[i] 
                                                              }, ignore_index=True)
        else:
          df_load_cycles_post = df_load_cycles_post.append({  'Equipo_id':df_load_cycles_1.Equipo_id[i],
                                                              'Equipo':df_load_cycles_1.Equipo[i],
                                                              'Blast_id':df_load_cycles_1.Blast_id[i],
                                                              'Blast':df_load_cycles_1.Blast[i],
                                                              'Dump_id':df_load_cycles_1.Dump_id[i],
                                                              'Dump':df_load_cycles_1.Dump[i],
                                                              'Categoria_origen':df_load_cycles_1.Categoria_origen[i],
                                                              'Categoria_destino':df_load_cycles_1.Categoria_destino[i],
                                                              'Tipo_trayecto': df_load_cycles_1.Tipo_trayecto[i],
                                                              'Tonelaje':df_load_cycles_1.Tonelaje[i],
                                                              'time_depart':df_load_cycles_1.time_depart[i],
                                                              'time_production_depart':df_load_cycles_1.time_production_depart[i],
                                                              'time_arrive':df_load_cycles_1.time_arrive[i], 
                                                              'time_production_arrive':df_load_cycles_1.time_production_arrive[i],
                                                              'time_spotting_and_spotted':df_load_cycles_1.time_spotting_and_spotted[i],
                                                              'time_production_spotting_and_spotted':df_load_cycles_1.time_production_spotting_and_spotted[i],
                                                              'time_stamp':df_load_cycles_1.time_stamp[i],
                                                              'time_production_stamp':df_load_cycles_1.time_production_stamp[i],
                                                              'traveling_time': df_load_cycles_1.traveling_time[i],
                                                              'inside_time':df_load_cycles_1.inside_time[i],
                                                              'Turno':df_load_cycles_1.Turno[i],
                                                              'Cuadrilla':df_load_cycles_1.Cuadrilla[i],
                                                              'index':str(df_load_cycles_1.time_depart[i].strftime('%y%m%d%H%M%S')) + str(int(df_load_cycles_1.Equipo_id[i])),
                                                              'travel_same_type':df_load_cycles_1.travel_same_type[i],
                                                              'time_origin_start':df_load_cycles_1.time_origin_start[i],
                                                              'inside_time_origin':df_load_cycles_1.inside_time_origin[i],
                                                              'case':  df_load_cycles_1.case[i],
                                                              'travel_distance': df_load_cycles_1.travel_distance[i],
                                                              'odometer_origin': df_load_cycles_1.odometer_origin[i],
                                                              'odometer_destination': df_load_cycles_1.odometer_destination[i]
                                                              }, ignore_index=True)
      else:
          df_load_cycles_post = df_load_cycles_post.append({  'Equipo_id':df_load_cycles_1.Equipo_id[i],
                                                              'Equipo':df_load_cycles_1.Equipo[i],
                                                              'Blast_id':df_load_cycles_1.Blast_id[i],
                                                              'Blast':df_load_cycles_1.Blast[i],
                                                              'Dump_id':df_load_cycles_1.Dump_id[i],
                                                              'Dump':df_load_cycles_1.Dump[i],
                                                              'Categoria_origen':df_load_cycles_1.Categoria_origen[i],
                                                              'Categoria_destino':df_load_cycles_1.Categoria_destino[i],
                                                              'Tipo_trayecto': df_load_cycles_1.Tipo_trayecto[i],
                                                              'Tonelaje':df_load_cycles_1.Tonelaje[i],
                                                              'time_depart':df_load_cycles_1.time_depart[i],
                                                              'time_production_depart':df_load_cycles_1.time_production_depart[i],
                                                              'time_arrive':df_load_cycles_1.time_arrive[i], 
                                                              'time_production_arrive':df_load_cycles_1.time_production_arrive[i],
                                                              'time_spotting_and_spotted':df_load_cycles_1.time_spotting_and_spotted[i],
                                                              'time_production_spotting_and_spotted':df_load_cycles_1.time_production_spotting_and_spotted[i],
                                                              'time_stamp':df_load_cycles_1.time_stamp[i],
                                                              'time_production_stamp':df_load_cycles_1.time_production_stamp[i],
                                                              'traveling_time': df_load_cycles_1.traveling_time[i],
                                                              'inside_time':df_load_cycles_1.inside_time[i],
                                                              'Turno':df_load_cycles_1.Turno[i],
                                                              'Cuadrilla':df_load_cycles_1.Cuadrilla[i],
                                                              'index':str(df_load_cycles_1.time_depart[i].strftime('%y%m%d%H%M%S')) + str(int(df_load_cycles_1.Equipo_id[i])),
                                                              'travel_same_type':df_load_cycles_1.travel_same_type[i],
                                                              'time_origin_start':df_load_cycles_1.time_origin_start[i],
                                                              'inside_time_origin':df_load_cycles_1.inside_time_origin[i],
                                                              'case':  df_load_cycles_1.case[i],
                                                              'travel_distance': df_load_cycles_1.travel_distance[i],
                                                              'odometer_origin': df_load_cycles_1.odometer_origin[i],
                                                              'odometer_destination': df_load_cycles_1.odometer_destination[i]
                                                              }, ignore_index=True)
  return df_load_cycles_post
