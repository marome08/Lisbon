import requests
import pandas as pd
from datetime import datetime, timedelta
import datetime
import json
import sys, os
import pyodbc
import Alternative_function


def change_is_reported_extraload_bd(sql_connect, travel_id_list):
    print('entro a la funcion que hace el cambio')
    query_update_is_reported = """UPDATE travels_extraload set is_reported=0 where travel_id=?"""

    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        try:
            new_cursor = connection.cursor()
            for travel_id in travel_id_list:
                new_cursor.execute(query_update_is_reported, travel_id)
            new_cursor.commit()
            print('Se realizo el cambio de los extraload')
        except Exception as err:
            print('Fallo la actualizacion de los reported extraload')
            Alternative_function.create_log_file('logAutomaticUncheckExtraloadUpdate.log', 'Fallo la actualizacion de los reported extraload')
            new_cursor.rollback()
            print(err)

def get_last_equipment_timestamp(sql_connect):
    """ generates a list of the teams that are working
        during the day. It also brings the time of the last existing record in the database.
        Args:
            sqlConnect  : array []
        Return: array []
    """
    query_select_last = """select equipment, MAX(time_stamp) as last_register_date 
                        from view_travel_powerby2
                        where filter='Current Day' and extraload=0
                        GROUP BY equipment
                        ORDER BY last_register_date"""
    equipment_list_timestamp = []
    try:
        with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
            new_cursor = connection.cursor()
            new_cursor.execute(query_select_last)
            last_timestamp_by_equipment_list =  new_cursor.fetchall()
            for equipment_info in last_timestamp_by_equipment_list:
                equipment_list_dicctionary = {'equipment_name': equipment_info[0], 'last_time_stamp':equipment_info[1]}
                equipment_list_timestamp.append(equipment_list_dicctionary)
    except Exception as err:
        print('Fallo la recoleccion de los reported del dia actual')
        Alternative_function.create_log_file('logAutomaticUncheckExtraloadGetTravelId.log','Fallo la recoleccion de travels')
        print(err)
    return equipment_list_timestamp

def get_travel_id_extraload(sql_connect, equipment_name, last_time_stamp):
    print(equipment_name)
    print(last_time_stamp)
    query_get_extraload_id = """select date, blast, dump, time_stamp, travel_id from view_travel_powerby2 
                                where filter='Current Day' and extraload=1 and is_reported=1 and 
                                equipment=? and time_stamp<=?"""
    travels_extraload_no_reported_list = []
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        try:
            new_cursor = connection.cursor()
            new_cursor.execute(query_get_extraload_id, equipment_name, last_time_stamp)
            travels_extraload_id = new_cursor.fetchall()
            for travels in travels_extraload_id:
                print(travels)
                travels_extraload_no_reported_list.append(travels[4])
        except Exception as err:
            print('fallo la recoleccion de travels_id extraload')
            Alternative_function.create_log_file('logAutomaticUncheckExtraloadGetMaxTimeStamp.log','Fallo la recoleccion de time stamp')
            print(err)
    return travels_extraload_no_reported_list

def main_reconciliation_load_production(connect_sqlServer):
    print('Analizando extraload checks')
    equipment_list = get_last_equipment_timestamp(connect_sqlServer)
    travel_id_no_reported_list = []
    for equipment_info in equipment_list:
        print(equipment_info['equipment_name'] + ' -- ' + str(equipment_info['last_time_stamp']))
        travels_extraload_list = get_travel_id_extraload(connect_sqlServer, equipment_info['equipment_name'], 
                                        equipment_info['last_time_stamp'])
        for travel_id in travels_extraload_list:
            travel_id_no_reported_list.append(travel_id)
    
    print(travel_id_no_reported_list)
    print(len(travel_id_no_reported_list))
    if(len(travel_id_no_reported_list)>0):
        change_is_reported_extraload_bd(connect_sqlServer, travel_id_no_reported_list)
    else:
        print('Lista vacia, por ende no hay que quitar el check a ningun extraload.')