from datetime import datetime, timedelta
import time, datetime
import pytz
import pyodbc


#Se debe agregar la nueva columna material_block_id [ok, falta test]
def insertSummaryTravelsByDates(sqlConnect, initialDate, endDate):
    # Se hace un barrido y se sube toda la informacion relacionada al summario de la tabla travels
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        queryInsertSummaryTravels = """INSERT INTO travel_production_daily_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        
        #Se filtra por columna is_reported
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
                #return
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
                materialBlockName = dataSummary[17]
                materialBlockId = dataSummary[18]
                material_class_id = dataSummary[19]
                if(material_class_id == None):
                    material_class_id = 1
                if(materialBlockId==None):
                    materialBlockId = 23
                shiftId = "0"
                if(dataSummary[10]=='Night'):
                    shiftId = "1"
                dailyId = yearDate + monthDate + dayDate + originId + destinationId + travelTypeId + crewId + shovelId + truckId + materialId + pitLevelId + shiftId +  rehandle + str(grade) + str(sulphu)  + str(materialBlockId) + str(material_class_id)
                #print(dataSummary)
                #print(dailyId)
                cursor.execute(queryInsertSummaryTravels, dailyId, date, destinationId, originId, travelTypeId,
                            journey, crewId, shovelId, truckId, materialId, pitLevelId, shift, grade,
                            sulphu, rehandle, trendAxis, totalPayload, totalTravels, materialBlockId, material_class_id)
            cursor.commit()
        except Exception as err:
            print(err)
            cursor.rollback()

#Se deben buscar diferencias utilizando la columna material_block_name
def searchErrorsSummaryTravelByDate(sqlConnect, initialDate, endDate):
    flagErrorFound = False
    
    queryGetAllTravelSummaryByDates = """Select daily_id, total_payload, total_travels 
        FROM travel_production_daily_summary WHERE daily_id=? AND total_travels=?;"""
    
    queryGetAllCountSummaryByDates = """SELECT COUNT(daily_id), ROUND(SUM(total_payload),2), 
        SUM(total_travels) FROM travel_production_daily_summary 
        WHERE day_shift>? AND day_shift<=?;"""
    
    #se filtra por columna is_reported
    querySelectInformationSummaryTravels = """SELECT date, travel_type_id, destination_location_id, origin_location_id, 
        journey, crew_id, equipment_c_id, equipment_t_id, material_id, pit_level_material_id, shift, grade, sulphu,
        rehandle, 1 as trend_axis, SUM(payload) as total_payload, COUNT(travel_id) as total_vueltas, material_block_name, block_names_record_id,
        material_class_id
        from view_all_historical_production_report
        where travel_type_name in ('Loaded', 'TBC') and is_reported=1 and deleted_at is null AND  date >= ? AND date<=?
        group by date, travel_type_id, destination_location_id, origin_location_id, journey, crew_id, equipment_c_id,
        equipment_t_id, material_id, pit_level_material_id, shift, grade, sulphu, rehandle, material_block_name, block_names_record_id, material_class_id;
        """
    try:
        with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
            cursor = conexion.cursor()
            cursor.execute(queryGetAllCountSummaryByDates, initialDate, endDate)
            countTravelsSummary = cursor.fetchone()
            print('Total registros Tabla resumen: ' + str(countTravelsSummary[0]))
            print('Total viajes Tabla resumen: ' + str(countTravelsSummary[2]))
            print('Total toneladas Tabla resumen: ' + str(countTravelsSummary[1]))
            cursor.execute(querySelectInformationSummaryTravels, initialDate, endDate)
            totalSummaryByTravels = cursor.fetchall()
            totalPayloadSummaryByTravels = 0
            totalTravelSummaryByTravels = 0
            for summaryRow in totalSummaryByTravels:
                totalPayloadSummaryByTravels = totalPayloadSummaryByTravels + summaryRow[15]
                totalTravelSummaryByTravels = totalTravelSummaryByTravels + summaryRow[16]
            totalPayloadSummaryByTravels = round(totalPayloadSummaryByTravels, 2)
            totalTravelSummaryByTravels = round(totalTravelSummaryByTravels, 2)
            print('Total registros Tabla original: ' + str(len(totalSummaryByTravels)))
            print('Total viajes Tabla original: ' + str(totalTravelSummaryByTravels))
            print('Total Payload Tabla original: ' + str(totalPayloadSummaryByTravels))
            
            if(countTravelsSummary[0] != len(totalSummaryByTravels) 
               or (countTravelsSummary[2] != totalTravelSummaryByTravels)
               or (countTravelsSummary[1] != totalPayloadSummaryByTravels)):
                flagErrorFound = True
                return flagErrorFound
        
            print('Cantidades de registros correctas')
            for summaryRow in totalSummaryByTravels:
                date = summaryRow[0]
                yearDate = str(date.year)
                monthDate = str(date.month)
                dayDate = str(date.day)
                travelTypeId = str(summaryRow[1])
                destinationId = str(summaryRow[2])
                originId = str(summaryRow[3])
                crewId = str(summaryRow[5])
                shovelId = str(summaryRow[6])
                truckId = str(summaryRow[7])
                materialId = str(summaryRow[8])
                pitLevelId = str(summaryRow[9])
                shiftId = "0"
                if(summaryRow[10]=='Night'):
                    shiftId = "1"
                totalPayload = summaryRow[15]
                totalTravels = summaryRow[16]
                rehandle = str(summaryRow[13])
                grade = str(summaryRow[11])
                sulphu = str(summaryRow[12])
                materialBlockId =  str(summaryRow[18])
                material_class_id = str(summaryRow[19])
                if(material_class_id == None):
                    material_class_id = 1
                if(materialBlockId == None):
                    materialBlockId = '23'
                dailyId = yearDate + monthDate + dayDate + originId + destinationId + travelTypeId + crewId + shovelId + truckId + materialId + pitLevelId + shiftId + rehandle + str(grade) + str(sulphu) + str(materialBlockId) + str(material_class_id)
                cursor.execute(queryGetAllTravelSummaryByDates, str(dailyId), totalTravels)
                resultComparation = cursor.fetchall()
                if(len(resultComparation)!=1):
                    print('Existen registros con problemas')
                    print(dailyId)
                    print(summaryRow)
                    flagErrorFound = True
                    break
        return flagErrorFound           
           
    except Exception as err:
        print('Error en exception')
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

#Por unos dias se debe intercambiar el valor de 
def reconciliationSummaryTravelByDateMain(sqlConnect):
    querySelectMaxShiftDate = """SELECT MAX(day_shift) FROM travel_production_daily_summary;"""
    #querySelectMaxShiftDate = """SELECT MIN(date) FROM view_travel_powerby2;"""
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        try:
            cursor.execute(querySelectMaxShiftDate)
            resulMaxShiftDate = cursor.fetchone()
            endDate = datetime.datetime(2024, 7, 1)
            if(len(resulMaxShiftDate)>0):
                endDate = resulMaxShiftDate[0]
            print(endDate)
            print(type(endDate))
            initialDate = endDate - datetime.timedelta(days=26)
            print(initialDate)
            foundErrors = searchErrorsSummaryTravelByDate(sqlConnect, initialDate, endDate)
            print(foundErrors)
            if(foundErrors):
                print('Existen errores')
                isDelete = deleteSummaryTravelByDate(sqlConnect, initialDate, endDate)
                if(isDelete):
                    insertSummaryTravelsByDates(sqlConnect, initialDate, endDate)
                    print('Proceso de reconciliacion Finalizado')
        except Exception as err:
            print(err)        

def insertSummaryTravelsMain(sqlConnect):
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        querySelectMaxShiftDate = """SELECT MAX(day_shift) FROM  travel_production_daily_summary;"""
        actualDatetimeSimberiAux = datetime.datetime.now(pytz.timezone('Pacific/Port_Moresby'))
        actualDatetimeSimberiAux = actualDatetimeSimberiAux - datetime.timedelta(hours=1)
        print(f"Datetime Pacific/Port_Moresby es: {actualDatetimeSimberiAux}")
        try:
            if(actualDatetimeSimberiAux.hour < 6):
                print('no a terminado el turno')
                #return 
            endDateSimberiAux = actualDatetimeSimberiAux - datetime.timedelta(1)
            endDate = endDateSimberiAux.strftime('%Y-%m-%d')
            cursor.execute(querySelectMaxShiftDate)
            resulMaxShiftDate = cursor.fetchone()
            print(endDate)
            initialDate = resulMaxShiftDate[0].strftime('%Y-%m-%d')
            if(initialDate ==  endDate):
                print('Es la misma fecha')
                return
            insertSummaryTravelsByDates(sqlConnect, initialDate, endDate)
        except Exception as err:
            print(err)
            return


#Despues se deben cambiar las credenciales
connect_sqlServer =['199.19.74.79', 'sa', 'Model+Mining+Simplefms', 'simplefms_lisbon_mine'] 
reconciliationSummaryTravelByDateMain(connect_sqlServer)
insertSummaryTravelsMain(connect_sqlServer)