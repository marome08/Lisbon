import pyodbc
import Alternative_function
from datetime import datetime, timedelta
import datetime
import Alternative_function

def GetOperatorIdByTravel(sql_connect, equipment_id, time_stamp):
    """ Obtains the identifier of the operator who made the trip
        Args:
            sqlConnect  : array []
            equipment_id: int
            time_stamp: datetime
        Return: int
    """
    operator_id = 20
    query_get_operator_id = """SELECT top 1 * from hist_operator_logged 
                            where equipment_id=? and (
                           created_at<=? and finished_at>=? OR 
                           (created_at<=? and finished_at is null))
                            order by created_at desc"""
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        try:
            cursorConnectionDB = connection.cursor()
            cursorConnectionDB.execute(query_get_operator_id, equipment_id, time_stamp,time_stamp,time_stamp)
            result_info_operator_id = cursorConnectionDB.fetchall()
            if(len(result_info_operator_id)<=0):
                return operator_id
            operator_id = result_info_operator_id[0][2]
            return operator_id
        except Exception as err:
            print(err)
            log_name_document = "log_assignment_operator_travels"
            log_message_document = "Equipment_id: " + str(equipment_id) + ' - Time_stamp: ' +  str(time_stamp)
            Alternative_function.create_log_file(log_name_document,log_message_document )
            return operator_id

def GetLocationIdByLocationName(sql_connect, location_name):
    """ Get the ids of the locations
        Args:
            sqlConnect  : array []
            location_name: string
        Return: int
    """
    query_get_locacion_id='select location_id from locations where name=?;'
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        cursorConnectionDB = connection.cursor()
        cursorConnectionDB.execute(query_get_locacion_id,location_name)
        result_query_location = cursorConnectionDB.fetchall()
        location_id = result_query_location[0][0]
        return location_id 
    
def GetEquipmentIdByEquipmentName(sql_connect, equipment_name):
    """ Get the ids of the teams
        Args:
            sqlConnect  : array []
            equipment_name: string
        Return: int
    """
    query_get_equipment_id='select equipment_id from equipment where equipment_name=?;'
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        cursorConnectionDB = connection.cursor()
        cursorConnectionDB.execute(query_get_equipment_id,equipment_name)
        result_query_equipment = cursorConnectionDB.fetchall()
        equipment_id = result_query_equipment[0][0]
        return equipment_id
    
def GetReasonIdByEquipmentId(sql_connect, equipment_id, travel_datetime):
    """ Obtain the identifiers of the reasons assigned to the trips.
        Args:
            sqlConnect  : array []
            equipment_id: int
            travel_datetime: datetime
        Return: int
    """
    query_get_reason_by_id = """SELECT reason_id FROM states where start_date<=? and 
                                (end_date>=? or end_date is null)
                                and equipment_id=?"""
    reason_id = None
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        cursorConnectionDB = connection.cursor()
        cursorConnectionDB.execute(query_get_reason_by_id, travel_datetime, travel_datetime, equipment_id)
        result_query_get_reason_by_id = cursorConnectionDB.fetchall()
        if(len(result_query_get_reason_by_id)<1):
            return reason_id
        
        reason_id = result_query_get_reason_by_id[0][0]
        return reason_id
    
def GetProductiveRoutesByOriginDestination(sql_connect, origin_geofence, destination_geofence, start_date, end_date):
    """ obtains the productive route for the trip made.
        Args:
            sql_connect          : array []
            origin_geofence      : string
            destination_geofence : string
            start_date           : datetime
            end_date             : datetime
        Return: array []
    """
    productive_route_data = []
    query_get_productive_routes_by_data = """select pr.blast_location_id, pr.destination_location_id, lo.name as origin, 
            ld.name as destination, pr.initial_date, pr.end_date, pr.is_active  from productive_routes pr
            INNER JOIN locations lo ON lo.location_id = pr.blast_location_id
            INNER JOIN locations ld ON ld.location_id = pr.destination_location_id  
            where is_active=1 AND lo.name=? AND ld.name=? AND pr.initial_date<=? 
            AND  (pr.end_date>=? or pr.end_date is null) order by initial_date desc"""
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
        cursorConnectionDB = connection.cursor()
        cursorConnectionDB.execute(query_get_productive_routes_by_data, origin_geofence, destination_geofence, start_date, end_date)
        result_query_get_productive_routes_by_data = cursorConnectionDB.fetchall()
        if(len(result_query_get_productive_routes_by_data)>0):
            productive_route_data = result_query_get_productive_routes_by_data
        return productive_route_data
    
def GetActualShiftDate(sql_connect):
    """ returns the date of the current shift
        Args:
            sql_connect : array []
        Return: datetime
    """
    query_get_current_shift = """select MAX(shift_date) from calendar_shift_crew where is_today=1"""
    current_shift_datetime = None
    try:
        with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as connection:
            cursorConnectionDB = connection.cursor()
            cursorConnectionDB.execute(query_get_current_shift)
            result_current_shift = cursorConnectionDB.fetchone()[0]
            print(result_current_shift)
            current_shift = datetime.datetime.combine(result_current_shift, datetime.datetime.min.time()).replace(hour=5)
            print(current_shift)
            current_shift_datetime = current_shift
            Alternative_function.telegramBot('La hora fue cambiada con exito para el corte de turno')
    except Exception as err:
        print(err)
        Alternative_function.telegramBot('Fallo el corte de turno a las 07:00 am')
    return current_shift_datetime


def Get_Backing_Or_Tipping_Signal(equipment, limit_start_date, limit_end_date, sql_connect, type_event):
    """ Function responsible for linking backing and tipping events with the stop in a geofence
        Args:
            equipment : int
            limit_start_date : datetime
            limit_end_date : datetime
            sql_connect : array []
            type_event : string
        Return: datetime, datetime, int, int, array
    """
    print('Se va a obtener la señal de ' + type_event)

    total_events = 0
    initial_datetime_event = None
    end_datetime_event = None
    duration_event = 0
    event_asociation_list = []
    query_get_information_event_by_datetimes ="""SELECT * FROM event_backing WHERE equipment_id=? and 
            time_start>=? AND time_end<=? order by time_start asc"""
    
    if(type_event == 'tipping'):
        query_get_information_event_by_datetimes ="""SELECT * FROM event_tipping WHERE equipment_id=? and 
            time_start>=? AND time_end<=? order by time_start asc"""

    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sql_connect[0]+';DATABASE='+sql_connect[3]+';UID='+sql_connect[1]+';PWD='+ sql_connect[2]) as conexion:
        try:
            cursor = conexion.cursor()
            cursor.execute(query_get_information_event_by_datetimes, equipment, limit_start_date, limit_end_date)
            result_information_backing = cursor.fetchall()
            total_events = len(result_information_backing)
            if(total_events==1):
                print('Solo un evento')
                event_backing = result_information_backing[0]
                print(event_backing)
                initial_datetime_event = event_backing[2]
                end_datetime_event = event_backing[3]
                duration_event =event_backing[4]
                event_asociation_list.append(event_backing[0])
            elif(total_events>1):
                print('varios eventos')
                firts_backing = result_information_backing[0]
                end_backing = result_information_backing[-1]

                initial_datetime_event = firts_backing[2]
                end_datetime_event = end_backing[3]
                duration_event = end_datetime_event - initial_datetime_event
                duration_event = duration_event.seconds

                for backing_event in result_information_backing:
                    event_asociation_list.append(backing_event[0])
                
                print(firts_backing)
                print(end_backing)
                
            else:
                print('Sin eventos')
        except Exception as err:
            print('Fallo la asociacion de eventos backing')
            print(err)

    return initial_datetime_event, end_datetime_event, duration_event, total_events, event_asociation_list