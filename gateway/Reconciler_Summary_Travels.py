from datetime import datetime, timedelta
import time, datetime
import pytz
import pyodbc

#funcion antigua
def deleteAction(sqlConnect, dailyId):
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        print(dailyId)
        queryGetDataByDailyId = """SELECT * FROM travel_production_daily_summary where daily_id=?;"""
        queryDeleteByDailyId = "DELETE FROM travel_production_daily_summary where daily_id=?;"
        queryInsertTravelsSummaryById = "INSERT INTO travel_production_daily_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        querySelectInformationSummaryTravels = """SELECT date, travel_type_id, destination_location_id, origin_location_id, 
            journey, crew_id, equipment_c_id, equipment_t_id, material_id, pit_level_material_id, shift, grade, sulphu,
            rehandle, 1 as trend_axis, SUM(payload) as total_payload, COUNT(travel_id) as total_vueltas
            from view_all_historical_production_report
            where travel_type_name in ('Loaded', 'TBC') AND is_reported=1 and deleted_at is null AND  date =? 
            AND travel_type_id=? AND destination_location_id=? AND origin_location_id=? AND crew_id=?
            AND equipment_c_id=? AND equipment_t_id=? AND material_id=? AND pit_level_material_id=? 
            AND shift=? AND grade=? AND sulphu=? AND rehandle=?
            group by date, travel_type_id, destination_location_id, origin_location_id, journey, crew_id, equipment_c_id,
            equipment_t_id, material_id, pit_level_material_id, shift, grade, sulphu, rehandle
            """
        try:
            cursor.execute(queryGetDataByDailyId, dailyId)
            rowDailyData = cursor.fetchone()
            print(rowDailyData)
            date = rowDailyData[2]
            yearDate = str(date.year)
            monthDate = str(date.month)
            dayDate = str(date.day)
            travelTypeId = str(rowDailyData[5])
            destinationId = str(rowDailyData[3])
            originId = str(rowDailyData[4])
            journey = rowDailyData[6]
            crewId = str(rowDailyData[7])
            shovelId = str(rowDailyData[8])
            truckId = str(rowDailyData[9])
            materialId = str(rowDailyData[10])
            pitLevelId = str(rowDailyData[11])
            shift = rowDailyData[12]
            grade = rowDailyData[13]
            sulphu = rowDailyData[14]
            rehandle = "0"
            if(rowDailyData[15]==1):
                rehandle = "1"
            print(rehandle)
            shiftId = "0"
            if(shift=='Night'):
                shiftId = "1"
            dailyId2 = yearDate + monthDate + dayDate + originId + destinationId + travelTypeId + crewId + shovelId + truckId + materialId + pitLevelId + shiftId +  rehandle + str(grade) + str(sulphu)
            print(dailyId)
            print(dailyId2)
            if(dailyId != dailyId2):
                return True
            cursor.execute(querySelectInformationSummaryTravels,
                        date, travelTypeId, destinationId, originId, crewId, shovelId,
                        truckId, materialId, pitLevelId, shift, grade, sulphu, rehandle)
            resultadoSummary =  cursor.fetchone()
            print(resultadoSummary)
            print('El id es el mismo')
            cursor.execute(queryDeleteByDailyId, dailyId)
            print('Se elimino el resumen del dailyId')
            cursor.execute(queryInsertTravelsSummaryById,
                            dailyId, date, destinationId, originId, travelTypeId, journey,
                            crewId, shovelId, truckId, materialId, pitLevelId, shift,
                            grade, sulphu, rehandle, 1, resultadoSummary[15],resultadoSummary[16])
            print('Se inserto el resumen')
            return False
        except Exception as err:
            print('Error al reconciliar data eliminada')
            print(err)
            cursor.rollback()
            return True

#funcion antigua
def insertAction(sqlConnect, travelId):
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        querySelectTravelById = """SELECT * FROM view_all_historical_production_report where travel_id=?"""
        querySelectSummaryByDailyId = """SELECT * FROM travel_production_daily_summary where daily_id=?; """
        queryInsertNewDailySummary = """"INSERT INTO travel_production_daily_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        querySelectInformationSummaryTravels = """SELECT date, travel_type_id, destination_location_id, origin_location_id, 
            journey, crew_id, equipment_c_id, equipment_t_id, material_id, pit_level_material_id, shift, grade, sulphu,
            rehandle, 1 as trend_axis, SUM(payload) as total_payload, COUNT(travel_id) as total_vueltas
            from view_all_historical_production_report
            where travel_type_name in ('Loaded', 'TBC') AND is_reported=1 and deleted_at is null AND  date =? 
            AND travel_type_id=? AND destination_location_id=? AND origin_location_id=? AND crew_id=?
            AND equipment_c_id=? AND equipment_t_id=? AND material_id=? AND pit_level_material_id=? 
            AND shift=? AND grade=? AND sulphu=? AND rehandle=?
            group by date, travel_type_id, destination_location_id, origin_location_id, journey, crew_id, equipment_c_id,
            equipment_t_id, material_id, pit_level_material_id, shift, grade, sulphu, rehandle
            """
        try:
            cursor.execute(querySelectTravelById, travelId)
            resultTravel = cursor.fetchone()
            print(resultTravel)
            date = resultTravel[3]
            yearDate = str(date.year)
            monthDate = str(date.month)
            dayDate = str(date.day)
            travelTypeId = str(resultTravel[10])
            destinationId = str(resultTravel[6])
            originId = str(resultTravel[8])
            journey = resultTravel[12]
            crewId = str(resultTravel[13])
            shovelId = str(resultTravel[15])
            truckId = str(resultTravel[17])
            materialId = str(resultTravel[19])
            pitLevelId = str(resultTravel[21])
            shift = resultTravel[23]
            grade = resultTravel[27]
            sulphu = resultTravel[28]
            rehandle = str(resultTravel[26])
            trendAxis = 1
            shiftId = "0"
            if(shift=='Night'):
                shiftId = "1"
            dailyId = yearDate + monthDate + dayDate + originId + destinationId + travelTypeId + crewId + shovelId + truckId + materialId + pitLevelId + shiftId +  rehandle + str(grade) + str(sulphu)
            print(dailyId)
            cursor.execute(querySelectSummaryByDailyId, dailyId)
            allDataSummaryById = cursor.fetchall()
            if(len(allDataSummaryById)==1):
                print('Se borra el daily y se agrega nuevamente')
                deleteAction(sqlConnect, dailyId)
                return False
            else:
                print('solo se debe ingresar el nuevo daily')
                cursor.execute(querySelectInformationSummaryTravels,
                        date, travelTypeId, destinationId, originId, crewId, shovelId,
                        truckId, materialId, pitLevelId, shift, grade, sulphu, rehandle)
                resultadoSummary =  cursor.fetchone()
                cursor.execute(queryInsertNewDailySummary,
                            dailyId, date, destinationId, originId, travelTypeId, journey,
                            crewId, shovelId, truckId, materialId, pitLevelId, shift,
                            grade, sulphu, rehandle, 1, resultadoSummary[15],resultadoSummary[16])
                cursor.commit()
                return False
        except Exception as err:
            cursor.rollback()
            print(err)
            return True

def deleteSummaryTravelByDate(sqlConnect, initialDate, endDate):
    isDelete = False
    with  pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        queryDeleteByDates = """DELETE FROM travel_production_daily_summary where day_shift>=? AND day_shift<=?;"""
        try:
            cursor.execute(queryDeleteByDates, initialDate, endDate)
            cursor.commit()
            print('Se eliminaron los registros, ya que, es necesaria una reconsiliacion de datos')
            isDelete = True
            return isDelete
        except:
            cursor.rollback()
            return isDelete

#Se debe modificar funcion [OK]
def insertSummaryTravelsByDates(sqlConnect, initialDate, endDate):
    # Se hace un barrido y se sube toda la informacion relacionada al summario de la tabla travels
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
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
                #El codigo dailyId sirve para identificar la fila sumarizada
                #Este codigo es util para compara columnas y descubrir si se realizo alguna modificacion en los datos
                #dailyId: Year + Month + Day + origin_id + destination_id + travel_type_id + crew_id + shovel_id + truck_id + material_id + pit_level_material_id + shift
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
                material_Class_id = dataSummary[19]
                if(materialBlockId==None):
                    materialBlockId = 23
                shiftId = "0"
                if(dataSummary[10]=='Night'):
                    shiftId = "1"
                dailyId = yearDate + monthDate + dayDate + originId + destinationId + travelTypeId + crewId + shovelId + truckId + materialId + pitLevelId + shiftId +  rehandle + str(grade) + str(sulphu) + str(materialBlockId) + str(material_Class_id)
                #print(dataSummary)
                #print(dailyId)
                cursor.execute(queryInsertSummaryTravels, dailyId, date, destinationId, originId, travelTypeId,
                            journey, crewId, shovelId, truckId, materialId, pitLevelId, shift, grade,
                            sulphu, rehandle, trendAxis, totalPayload, totalTravels, materialBlockId, material_Class_id)
            cursor.commit()
            isInsert = True
            return isInsert
        except Exception as err:
            print(err)
            cursor.rollback()
            isInsert = False
            return isInsert

def reconciliationSummaryTravelsActionsLastVersion(sqlConnect):
    #Primero se consulta a la tabla 3 si existe algun proceso nuevo para analizar
    #Si no existe proceso se genera un return y termina esta funcion
    #Si existe un proceso se debe revisar, ejecutar dado el caso que represente y se debe actualizar el estado de la tabla 3
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        queryTableActions = """SELECT * FROM reconciler_tables_actions WHERE status='TBC';"""
        queryUpdateDeleteAction = """UPDATE reconciler_tables_actions set status=?, ending_date=? where reconciler_tables_actions_id=?;"""
        try:
            cursor.execute(queryTableActions)
            resultTableActions = cursor.fetchall()
            if(len(resultTableActions)==0):
                print('es cero')
                return
            for newAxtion in resultTableActions:
                print(newAxtion)
                resulExecution = 'Completed'
                isNotCompleted =  None
                if(newAxtion[2]=='Delete'):
                    print('Se debe eliminar todo los datos sumarizados para ese daily_id')
                    isNotCompleted = deleteAction(sqlConnect, newAxtion[6])  
                if(newAxtion[2]=='Insert'):
                    print('Se debe revisar si existe este dato sumarizado, de existir se borra el daily_id y se reingresa la informacion')
                    isNotCompleted = insertAction(sqlConnect, newAxtion[6])
                if(newAxtion[2]=='Update'):
                    print('Se debe actualizar la data')
                    querySelectDate = """SELECT * FROM travel_production_daily_summary where daily_id=?"""
                    try:
                        cursor.execute(querySelectDate, newAxtion[6])
                        result = cursor.fetchone()
                        print(result)
                        print('\n')
                        isDelete = deleteSummaryTravelByDate(sqlConnect, result[2], result[2])
                        if(isDelete):
                            insertSummaryTravelsByDates(sqlConnect, result[2], result[2])
                            isNotCompleted = False
                        else:
                            isNotCompleted = True
                    except Exception as err:
                        print(err)
                        isNotCompleted = True
                if(isNotCompleted):
                    print('No se realizo el cambio')
                    resulExecution = 'Error'
                endingDateAux = datetime.datetime.now()
                endingDate = endingDateAux.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(queryUpdateDeleteAction, resulExecution, endingDate, newAxtion[0])
            print('hola')
        except Exception as err:
            print(err)

#Esta el la funcion principal, no se debe hacer ningun camnbio
def reconciliationSummaryTravelsActions(sqlConnect):
    #Primero se consulta a la tabla 3 si existe algun proceso nuevo para analizar
    #Si no existe proceso se genera un return y termina esta funcion
    #Si existe un proceso se debe revisar, ejecutar dado el caso que represente y se debe actualizar el estado de la tabla 3
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        queryTableActions = """SELECT * FROM reconciler_tables_actions WHERE status='TBC' order by reconciler_tables_actions_id asc;"""
        queryUpdateDeleteAction = """UPDATE reconciler_tables_actions set status=?, ending_date=? where reconciler_tables_actions_id=?;"""
        queryGetDateById = """SELECT date from view_all_historical_production_report where travel_id=?"""
        updateAllTableActions= False
        try:
            cursor.execute(queryTableActions)
            resultTableActions = cursor.fetchall()
            if(len(resultTableActions)==0):
                print('es cero')
                return
            for newAxtion in resultTableActions:
                print(newAxtion)
                resulExecution = 'No Completed'
                cursor.execute(queryGetDateById, newAxtion[6])
                dateById = cursor.fetchone()
                print(dateById)
                if(dateById==None):
                    print('travelId es 0')
                else:
                    isDelete = deleteSummaryTravelByDate(sqlConnect, dateById[0], dateById[0])
                    if(isDelete):
                        isInsert = insertSummaryTravelsByDates(sqlConnect, dateById[0], dateById[0])
                        if(isInsert):
                            resulExecution = 'Completed'
                            updateAllTableActions = True
                endingDateAux = datetime.datetime.now()
                endingDate = endingDateAux.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(queryUpdateDeleteAction, resulExecution, endingDate, newAxtion[0])
            print('hola')
            if(updateAllTableActions):
                cursor.execute(queryTableActions)
                resultTableActions = cursor.fetchall()
                if(resultTableActions!=None):
                    for newAxtion in resultTableActions:
                        endingDateAux = datetime.datetime.now()
                        endingDate = endingDateAux.strftime("%Y-%m-%d %H:%M:%S")
                        cursor.execute(queryUpdateDeleteAction, 'Completed', endingDate, newAxtion[0])
        except Exception as err:
            print(err)
