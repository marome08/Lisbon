from datetime import datetime, timedelta
import datetime
import json
import sys, os
import pyodbc
import Alternative_function 

def RecordTumCorrection(sql_connect, equipments_with_problems_list):
    """ The system autocorrects the tum record, so that it is active again
        Args:
            sqlConnect  : array []
            equipments_with_problems_list : array []
        Return: Boolean
    """
    get_last_status_equipment = """select top 10 * from states where equipment_id=?
                                    order by start_date desc
                                """
    insert_last_status_equipment = """INSERT INTO states values (?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    is_solved = False
    total_equipment_with_problems = len(equipments_with_problems_list)
    total_record_insert = 0
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        try:
            created_at_utc = datetime.datetime.utcnow()
            datetime_simberi = created_at_utc + datetime.timedelta(hours=10)
            datetime_simberi_transform = datetime_simberi.replace(second=0, microsecond=0)
            cursor_connection_bd =  connection.cursor()
            for equipment_id in equipments_with_problems_list:
                cursor_connection_bd.execute(get_last_status_equipment, equipment_id)
                result_last_status_equipment =  cursor_connection_bd.fetchone()
                print(result_last_status_equipment)
                reason_id = result_last_status_equipment[2]
                start_date = result_last_status_equipment[4]
                end_date = None
                status = 1
                user1_comment = result_last_status_equipment[6]
                user1 = result_last_status_equipment[7]
                user2 = result_last_status_equipment[8]
                id_automatic_event = result_last_status_equipment[9]
                user2_comment = result_last_status_equipment[10]
                original_start_date = result_last_status_equipment[4]
                created_at = datetime_simberi_transform
                created_by = 'Tum Gateway System'
                cursor_connection_bd.execute(insert_last_status_equipment, equipment_id, reason_id, start_date, end_date, status,
                                            user1_comment, user1, user2, id_automatic_event, user2_comment, original_start_date,
                                            created_at, created_by)
                cursor_connection_bd.commit()
                total_record_insert = total_record_insert + cursor_connection_bd.rowcount
            
            if(total_equipment_with_problems == total_record_insert):
                is_solved = True
            else:
                is_solved = False
            return is_solved
        except Exception as err:
            print(err)
            return is_solved

def CompareListEquipmentTum(equipments_list, equipments_list_tum):
    """ Compare the two lists to identify problem computers.
        Args:
            equipments_list  : array []
            equipments_list_tum  : array []
        Return: Boolean, Array []
    """
    equipments_with_problems = []
    total_equipment_list = len(equipments_list)
    total_equipment_list_tum = len(equipments_list_tum)

    if(total_equipment_list==total_equipment_list_tum and total_equipment_list>0 and total_equipment_list_tum>0):
        return False, equipments_with_problems
    
    set_equipment_list = set(equipments_list)
    set_equipment_list_tum = set(equipments_list_tum)

    aux_equipments_with_problems = set_equipment_list_tum.symmetric_difference(set_equipment_list)
    for x in aux_equipments_with_problems:
        equipments_with_problems.append(x)
    
    return True, equipments_with_problems

def GetEquipmentActiveTum(sql_connect):
    """ Obtains the list of active devices in the system and active devices in the tum
        Args:
            sqlConnect  : array []
        Return: Two Array []
    """
    data_equipment_list_active = []
    data_equipment_list_active_in_tum = []
    get_equipment_list_active = """select equipment_id from equipment where equipment_active = 1
                                    order by equipment_id"""
    get_equipment_list_active_in_tum = """select equipment_id from states where status=1
                                            order by equipment_id"""
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        try:
            cursor_connection_bd = connection.cursor()
            cursor_connection_bd.execute(get_equipment_list_active)
            aux_data_equipment_list_active = cursor_connection_bd.fetchall()

            for row in aux_data_equipment_list_active:
                data_equipment_list_active.append(row[0])
            cursor_connection_bd.execute(get_equipment_list_active_in_tum)
            aux_data_equipment_list_active_in_tum = cursor_connection_bd.fetchall()
            for row in aux_data_equipment_list_active_in_tum:
                data_equipment_list_active_in_tum.append(row[0])
            return data_equipment_list_active, data_equipment_list_active_in_tum
        except Exception as err:
            print(err)
            return data_equipment_list_active, data_equipment_list_active_in_tum

def MainAlertTum(connect_sqlServer):
    """ The equipment review process begins at the TUM.
        Args:
            connect_sqlServer  : array []
    """
    data_equipment_list_active, data_equipment_list_active_in_tum = GetEquipmentActiveTum(connect_sqlServer)
    is_problem, equipment_with_problems =CompareListEquipmentTum(data_equipment_list_active, data_equipment_list_active_in_tum)
    if(is_problem==True):
        total_equipments_with_problemas = len(equipment_with_problems)
        aux_message_equipment_list = ': '
        for equipment in equipment_with_problems:
            aux_message_equipment_list = aux_message_equipment_list + str(equipment) + ', ' 
        message_equipment_with_problemns = 'Se detectaron ' + str(total_equipments_with_problemas) + ' equipos con conflictos en el TUM' + aux_message_equipment_list
        print(message_equipment_with_problemns)
        Alternative_function.create_log_file('equiposDistntosTum.log', message_equipment_with_problemns)

        Alternative_function.telegramBot(message_equipment_with_problemns)
        #Aqui va el codigo de telegram.
        #is_solved = RecordTumCorrection(connect_sqlServer, equipment_with_problems)
        #if(is_solved==True):
        #    print('Los equipos fueron autocorregidos.')
    
