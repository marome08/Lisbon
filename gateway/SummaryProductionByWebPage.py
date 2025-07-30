import datetime
import pytz
import pyodbc

def deleteSummaryTravelByDate(cursor, initial_date, end_date):
    """ Delete records from the travel_production_daily_summary table between the selected date range.
        Args:
            cursor : pyodbc.Cursor 
            initial_date : datetime
            end_date : datetime
        Return: Boolean
    """
    isDelete = False
    queryDeleteByDates = """DELETE FROM travel_production_daily_summary where day_shift>=? AND day_shift<=?;"""
    try:
        cursor.execute(queryDeleteByDates, initial_date, end_date)
        print('Se eliminaron los registros, ya que, es necesaria una reconsiliacion de datos')
        isDelete = True
        return isDelete
    except:
        return isDelete

def insertSummaryTravelsByDates(cursor, initialDate, endDate):
    """ inserts the summary records into the travel_production_daily_summary table between the selected date range
        Args:
            cursor : pyodbc.Cursor 
            initial_date : datetime
            end_date : datetime
        Return: Boolean
    """
    isInsert = True
    queryInsertSummaryTravels = """INSERT INTO travel_production_daily_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    querySelectInformationSummaryTravels = """SELECT date, travel_type_id, destination_location_id, origin_location_id, 
                journey, crew_id, equipment_c_id, equipment_t_id, material_id, pit_level_material_id, shift, grade, sulphu,
                rehandle, 1 as trend_axis, SUM(payload) as total_payload, COUNT(travel_id) as total_vueltas,  material_block_name,
                block_names_record_id, material_class_id
                from view_all_historical_production_report
                where travel_type_name in ('Loaded', 'TBC') AND is_reported=1 and deleted_at is null AND  date >=? AND date<=?
                group by date, travel_type_id, destination_location_id, origin_location_id, journey, crew_id, equipment_c_id,
                equipment_t_id, material_id, pit_level_material_id, shift, grade, sulphu, rehandle,  material_block_name,
                block_names_record_id, material_class_id
            """
    try:
        cursor.execute(querySelectInformationSummaryTravels,initialDate,endDate)
        resultAllDataSummary = cursor.fetchall()
        print('Se recolecto la informacion')
        if(len(resultAllDataSummary)==0):
            print('Es cero')
            isInsert = False
            return isInsert
        for dataSummary in resultAllDataSummary:
            date = dataSummary[0]
            yearDate = str(date.year)
            monthDate = str(date.month)
            dayDate = str(date.day)
            travelTypeId = str(dataSummary[1])
            destinationId = str(dataSummary[2])
            originId = str(dataSummary[3])
            journey = dataSummary[4]
            crewId = str(dataSummary[5])
            shovelId = str(dataSummary[6])
            truckId = str(dataSummary[7])
            materialId = str(dataSummary[8])
            pitLevelId = str(dataSummary[9])
            shift = dataSummary[10]
            grade = dataSummary[11]
            sulphu = dataSummary[12]
            rehandle = str(dataSummary[13])
            trendAxis = dataSummary[14]
            totalPayload = dataSummary[15]
            totalTravels = dataSummary[16]
            materialBlockId = dataSummary[18]
            material_class_id =  dataSummary[19]
            if(materialBlockId==None):
                materialBlockId = 23
            shiftId = "0"
            if(dataSummary[10]=='Night'):
                shiftId = "1"
            dailyId = yearDate + monthDate + dayDate + originId + destinationId + travelTypeId + crewId + shovelId + truckId + materialId + pitLevelId + shiftId +  rehandle + str(grade) + str(sulphu) + str(materialBlockId) + str(material_class_id) 
            cursor.execute(queryInsertSummaryTravels, dailyId, date, destinationId, originId, travelTypeId, journey, crewId, shovelId, 
                           truckId, materialId, pitLevelId, shift, grade, sulphu, rehandle, trendAxis, totalPayload, totalTravels, materialBlockId, material_class_id)
        isInsert = True
        return isInsert
    except Exception as err:
        print(err)
        isInsert = False
        return isInsert

def UpdateSummaryTravelsActions(cursor, settings_summary_actions_id, status_action):
    """ inserts the summary records into the travel_production_daily_summary table between the selected date range
        Args:
            cursor : pyodbc.Cursor 
            settings_summary_actions_id : int
            status_action : string
        Return: Boolean
    """
    time_zone_simberi = pytz.timezone('Pacific/Port_Moresby')
    simberi_now_datetime = datetime.datetime.now(time_zone_simberi)
    simberi_now_datetime_formated = simberi_now_datetime.strftime("%Y-%m-%d %H:%M:%S")
    print("Fecha y hora actual en Pacific/Port_Moresby: ", simberi_now_datetime_formated)
    query_update_summary_actions = """UPDATE settings_summary_actions SET status_action=?, updated_at=? WHERE settings_summary_actions_id=?"""
    cursor.execute(query_update_summary_actions, status_action, simberi_now_datetime_formated, settings_summary_actions_id)
    cursor.commit()

#Funcion principal/Encargada de iniciar la ejecucion del proceso
def MainReconciliationSummaryTravelsActions(sqlConnect):
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        query_get_summary_actions = """SELECT * FROM settings_summary_actions WHERE status_action='TBC' ORDER BY settings_summary_actions_id"""

        try:
            cursor.execute(query_get_summary_actions)
            summary_actions_list = cursor.fetchall()
            if(len(summary_actions_list)==0):
                print('es cero')
                return
            for summary_action in summary_actions_list:
                print(summary_action)
                action_status = 'Completed'
                if(summary_action[1]=='travel_production_daily_summary'):
                    start_date = summary_action[5]
                    end_date = summary_action[6]
                    is_delete = deleteSummaryTravelByDate(cursor, start_date, end_date)
                    if(is_delete):
                        is_insert = insertSummaryTravelsByDates(cursor, start_date, end_date)
                        if(is_insert):
                            print('Se debe actualizar la columna')
                            cursor.commit()
                        else:
                            action_status = 'No completed'
                            cursor.rollback()
                    else:
                        action_status = 'No completed'
                        cursor.rollback()
                    UpdateSummaryTravelsActions(cursor, summary_action[0],action_status)
            cursor.close()

        except Exception as err:
            print(err)
            cursor.rollback()
            cursor.close()
