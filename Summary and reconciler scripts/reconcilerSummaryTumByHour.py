import warnings
import pandas as pd
from datetime import datetime, timedelta
import time, datetime
import json
import pyodbc
import pytz
warnings.simplefilter('ignore')

def solverCaseC(stateList):
    finalTUMList = []
    try:
        #print('Se ejecuta el caso C')
        stateId = stateList[0]
        equipmentId = stateList[1]
        reasonId = stateList[8]
        startDate = stateList[10]
        endDate = stateList[11]
        shiftDate = stateList[13]
        newHour = startDate.hour + 1
        newDate = startDate.replace(hour=newHour, minute=0, second=0)
        #print(newDate)
        diferenciaTUM1 = newDate - startDate
        segundosTUM1 = diferenciaTUM1.total_seconds()

        diccStateTUM1 = {'state':stateId, 'equipmentId':equipmentId, 'reasonId':reasonId, 
                            'startDate':startDate, 'endDate': newDate, 'seconds': segundosTUM1,'hourDate': startDate.hour,
                            'shiftDate': shiftDate}
        
        diferenciaTUM2 = endDate - newDate
        segundosTUM2 = diferenciaTUM2.total_seconds()

        diccStateTUM2 = {'state':stateId, 'equipmentId':equipmentId, 'reasonId':reasonId, 
                            'startDate':newDate, 'endDate': endDate, 'seconds': segundosTUM2,'hourDate': newDate.hour,
                            'shiftDate': shiftDate}
        finalTUMList.append(diccStateTUM1)
        finalTUMList.append(diccStateTUM2)
        return finalTUMList
    except Exception as err:
        emptyList = []
        return emptyList

#ok
def solverCaseD(stateList):
    finalTUMList = []
    try:
        #print('Se ejecuta el caso D')
        stateId = stateList[0]
        equipmentId = stateList[1]
        reasonId = stateList[8]
        startDate = stateList[10]
        endDate = stateList[11]
        shiftDate = stateList[13]
        hourList = list(range(startDate.hour, endDate.hour + 1))
        #print(hourList)
        for indice, valor in enumerate(hourList):
            startDateAux = None
            endDateAux = None 
            if indice == 0:
                #print("Estás en el primer valor:", valor)
                startDateAux = startDate
                endDateAux = startDate.replace(hour=valor + 1 , minute=0, second=0)
            elif indice == len(hourList) - 1:
                #print("Estás en el último valor:", valor)
                startDateAux = startDate.replace(hour=valor, minute=0, second=0)
                endDateAux = endDate
            else:
                #print("Estás en un valor intermedio:", valor)
                startDateAux = startDate.replace(hour=valor, minute=0, second=0)
                endDateAux = startDate.replace(hour=valor + 1, minute=0, second=0)
            
            diferenciaTUM = endDateAux - startDateAux
            segundosTUM = diferenciaTUM.total_seconds()
            diccStateTUM = {'state':stateId, 'equipmentId':equipmentId, 'reasonId':reasonId, 
                            'startDate':startDateAux, 'endDate': endDateAux, 'seconds': segundosTUM,'hourDate': startDateAux.hour,
                            'shiftDate': shiftDate}
            #print(diccStateTUM)
            finalTUMList.append(diccStateTUM)
        return finalTUMList
    except Exception as err:
        emptyList = []
        return emptyList

#ok
def solverCaseE(stateList):
    #print('Se ejecuta el caso E')
    finalTUMList = []
    try:
        stateId = stateList[0]
        equipmentId = stateList[1]
        reasonId = stateList[8]
        startDate = stateList[10]
        endDate = stateList[11]
        shiftDate = stateList[13]
        auxNewDay = startDate + datetime.timedelta(days=1)
        #print(auxNewDay)
        auxNewDayFull = auxNewDay.replace(hour=0 , minute=0, second=0)
        #print(auxNewDayFull)
        hourList = list(range(startDate.hour, 24))
        #print(hourList)
        hourList2 = list(range(auxNewDayFull.hour, endDate.hour + 1))
        #print(hourList2)
        if(startDate.hour==23):
            startDateAux = startDate
            endDateAux = auxNewDayFull.replace(hour=00 , minute=0, second=0)
            diferenciaTUM = endDateAux - startDateAux
            segundosTUM = diferenciaTUM.total_seconds()
            diccStateTUM = {'state':stateId, 'equipmentId':equipmentId, 'reasonId':reasonId, 
                            'startDate':startDateAux, 'endDate': endDateAux, 'seconds': segundosTUM,'hourDate': startDateAux.hour,
                            'shiftDate': shiftDate}
            #print(diccStateTUM)
            finalTUMList.append(diccStateTUM)
        else:    
            for indice, valor in enumerate(hourList):
                startDateAux = None
                endDateAux = None 
                if indice == 0:
                    startDateAux = startDate
                    endDateAux = startDate.replace(hour=valor + 1 , minute=0, second=0)
                elif indice == len(hourList) - 1:
                    startDateAux = startDate.replace(hour=valor, minute=0, second=0)
                    endDateAux = auxNewDayFull
                else:
                    startDateAux = startDate.replace(hour=valor, minute=0, second=0)
                    endDateAux = startDate.replace(hour=valor + 1, minute=0, second=0)
                
                diferenciaTUM = endDateAux - startDateAux
                segundosTUM = diferenciaTUM.total_seconds()
                diccStateTUM = {'state':stateId, 'equipmentId':equipmentId, 'reasonId':reasonId, 
                                'startDate':startDateAux, 'endDate': endDateAux, 'seconds': segundosTUM,'hourDate': startDateAux.hour,
                                'shiftDate': shiftDate}
                #print(diccStateTUM)
                finalTUMList.append(diccStateTUM)
        
        for indice, valor in enumerate(hourList2):
            startDateAux = None
            endDateAux = None 
            if indice == 0:
                startDateAux = auxNewDayFull
                if(endDate.hour==0):
                    endDateAux = endDate
                else:
                    endDateAux = auxNewDayFull.replace(hour=valor + 1 , minute=0, second=0)
            elif indice == len(hourList2) - 1:
                startDateAux = auxNewDayFull.replace(hour=valor, minute=0, second=0)
                endDateAux = endDate
            else:
                startDateAux = auxNewDayFull.replace(hour=valor, minute=0, second=0)
                endDateAux = auxNewDayFull.replace(hour=valor + 1, minute=0, second=0)
                
            diferenciaTUM = endDateAux - startDateAux
            segundosTUM = diferenciaTUM.total_seconds()
            diccStateTUM = {'state':stateId, 'equipmentId':equipmentId, 'reasonId':reasonId, 
                            'startDate':startDateAux, 'endDate': endDateAux, 'seconds': segundosTUM,'hourDate': startDateAux.hour,
                            'shiftDate': shiftDate}
            #print(diccStateTUM)
            finalTUMList.append(diccStateTUM)
        return finalTUMList
    except Exception as err:
        emptyList = []
        return emptyList

#ok
def separacionStateByHour(stateList):
    finalTUMList = []
    try:
        stateId = stateList[0]
        equipmentId = stateList[1]
        reasonId = stateList[8]
        reasonName = stateList[5]
        startDate = stateList[10]
        endDate = stateList[11]
        shiftDate = stateList[13]
        startDateDay = startDate.day
        endDateDay = endDate.day
        startDateHour = startDate.hour
        endDateHour = endDate.hour
        endDateMinutes = endDate.minute
        endDateSecond = endDate.second
        diferenceBetweenStartEndHour = endDateHour - startDateHour    
        #Case 1: StartDate y EndDate tienen la misma hora
        if(startDateDay == endDateDay and startDateHour==endDateHour):
            #print('Case A')
            diferenciaTUM = endDate - startDate
            segundosTUM = diferenciaTUM.total_seconds()
            diccStateTUM = {'state':stateId, 'equipmentId':equipmentId, 'reasonId':reasonId, 
                            'startDate':startDate, 'endDate': endDate, 'seconds':segundosTUM,'hourDate': startDateHour,
                            'shiftDate': shiftDate}
            finalTUMList.append(diccStateTUM)
        elif(startDateDay == endDateDay and diferenceBetweenStartEndHour==1 and endDateMinutes==0 and endDateSecond==0):
            #print('Case B')
            diferenciaTUM = endDate - startDate
            segundosTUM = diferenciaTUM.total_seconds()
            diccStateTUM = {'state':stateId, 'equipmentId':equipmentId, 'reasonId':reasonId, 
                            'startDate':startDate, 'endDate': endDate, 'seconds':segundosTUM ,'hourDate': startDateHour,
                            'shiftDate': shiftDate}
            finalTUMList.append(diccStateTUM)
        elif(startDateDay == endDateDay and diferenceBetweenStartEndHour==1):
            stateListCorrect = solverCaseC(stateList)
            for correctState in stateListCorrect:
                finalTUMList.append(correctState)
        elif(startDateDay == endDateDay and diferenceBetweenStartEndHour>1):
            stateListCorrect = solverCaseD(stateList)
            for correctState in stateListCorrect:
                finalTUMList.append(correctState)
        elif(startDateDay != endDateDay):
            correctStateList = solverCaseE(stateList)
            for correctState in correctStateList:
                finalTUMList.append(correctState)
        else:
            print('######################### Otro caso #########################')
            print(stateId)
            print(equipmentId)
            print(reasonId)
            print(reasonName)
            print(startDate)
            print(endDate)
            print(shiftDate)
            print(startDate.hour)
            print('Dia inicial: ' + str(startDateDay))
            print('Dia final: ' + str(endDateDay))
            print('Hora inicial:' + str(startDateHour))
            print('Hora final:' + str(endDateHour))
            print('Minuto final: ' + str(endDateMinutes))
            print('segundo final: ' + str(endDateSecond))
        return finalTUMList
    except Exception as err:
        emptyList = []
        return emptyList

def tranformacionTumByHour(sqlConnect, equipmentId, startDate, endDate):
    listTUM2 = []
    
    queryGetstates = """SELECT * FROM view_hist_states where shift_date >= ? and shift_date <= ? 
                        and equipment_id=? order by start_date asc;"""
    resultGetstates = None
    try:
        with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
            cursor = conexion.cursor()
            print('Equipo: ' + str(equipmentId))
            if(startDate == endDate):
                queryGetstates = """SELECT * FROM view_hist_states where shift_date =  ?
                            and equipment_id=? order by start_date asc;"""
                cursor.execute(queryGetstates, startDate, equipmentId)
                resultGetstates = cursor.fetchall()
            else:
                cursor.execute(queryGetstates, startDate, endDate, equipmentId)
                resultGetstates = cursor.fetchall()
            if(len(resultGetstates)<=0):
                return
            for state in resultGetstates:
                statesCorrect = separacionStateByHour(state)
                if(len(statesCorrect)>0):
                    for newState in statesCorrect:
                        if(newState['seconds'] != 0 ):
                            listTUM2.append(newState)
        return listTUM2
    except:
        emptyTravel = []
        return emptyTravel

def searchErrorSummaryTUMByHour(sqlConnect):
    listEquipmentErrors = []
    queryGetEquipments = """SELECT equipment_id from equipment where equipment_active=1"""
    queryGetstates = """select * from tum_by_hours where shift_date>=? and equipment_id=? order by shift_date asc"""
    queryGetMaxDate = """SELECT MAX(shift_date) from tum_by_hours;"""
    try:
        with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
            cursor = conexion.cursor()
            cursor.execute(queryGetEquipments)
            equipmentsList = cursor.fetchall()
            cursor.execute(queryGetMaxDate)
            endDateTable = cursor.fetchone()
            actualDatetimeSimberiAux = datetime.datetime.now(pytz.timezone('America/Denver'))
            auxtime = actualDatetimeSimberiAux - datetime.timedelta(days=30)
            startDate = auxtime.strftime('%Y-%m-%d')
            endDate = actualDatetimeSimberiAux.strftime('%Y-%m-%d')
            print(startDate)
            print(endDate)
            print(endDateTable[0])
            
            for equipment in equipmentsList:
                listTumOriginal = []
                listTumSummary = []
                #print(equipment[0])
                listTumOriginal = tranformacionTumByHour(sqlConnect, equipment[0], startDate, endDateTable[0])
                #print(listTumOriginal)
                cursor.execute(queryGetstates, startDate, equipment[0])
                statesByHour = cursor.fetchall()
                for state in statesByHour:
                    diccState = {'state':state[1], 'equipmentId':state[2], 'reasonId':state[3], 
                            'startDate':state[4], 'endDate': state[5], 'seconds': float(state[9]),'hourDate': state[8],
                            'shiftDate': state[6]}
                    listTumSummary.append(diccState)
                #print(listTumSummary)
                if(listTumOriginal != None):
                    print(len(listTumSummary))

                    print(len(listTumOriginal))
                    for dict1, dict2 in zip(listTumOriginal, listTumSummary):
                        if dict1 != dict2:
                            print("Los diccionarios son diferentes.")
                            print(dict1)
                            print(dict2)
                            print('------------------------------------------')
                            diccEquipmentError = {'Equipment_id': dict1['equipmentId'], 'shift_date':dict1['shiftDate']}
                            listEquipmentErrors.append(diccEquipmentError)
        return listEquipmentErrors
    except Exception as err:
        emptyList = []
        print(err)
        return emptyList

def insertTumByHour(sqlConnect, tumByHourList):
    queryInsertTumByHour = """INSERT INTO tum_by_hours VALUES (?,?,?,?,?,?,?,?,?);"""
    try:
        with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
            cursor = conexion.cursor()
            print(tumByHourList)
            for tumByHour in tumByHourList:
                print(tumByHour)
                shift = 'Night'
                if(tumByHour['hourDate'] >= 6 and tumByHour['hourDate'] <18):
                    shift = 'Day'
                
                cursor.execute(queryInsertTumByHour, tumByHour['state'], tumByHour['equipmentId'], tumByHour['reasonId'],
                                tumByHour['startDate'], tumByHour['endDate'], tumByHour['shiftDate'], shift,tumByHour['hourDate'],
                                tumByHour['seconds'])
            cursor.commit()
            cursor.close()
            print('elimino los datos')
    except Exception as err:
        print(err)

def reconciliadorTumByHour(sqlConnect, errorList):
    print('Entro a borrar datos')
    queryDeleteTumByHour = """delete from tum_by_hours where equipment_id=? and shift_date=?"""
    #falta un for
    equipmentId = errorList[0]['Equipment_id']
    shiftDate = errorList[0]['shift_date']
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        try:
            cursor = conexion.cursor()
            cursor.execute(queryDeleteTumByHour, equipmentId, shiftDate)
            print(errorList[0])
            print(equipmentId)
            print(shiftDate)
            tumHourList = tranformacionTumByHour(sqlConnect, equipmentId, shiftDate, shiftDate)
            print(tumHourList)
            if(len(tumHourList) > 0):  
                print('entro al insert')  
                insertTumByHour(sqlConnect,tumHourList)
                cursor.commit()
        except Exception as err:
            print(err)
    
def mainReconciliadorTUmByHour(sqlConnect):
    try:
        flag_is_ok = False
        while flag_is_ok==False:
            print('Main Reconciliador')
            equipmentByDayError  = searchErrorSummaryTUMByHour(sqlConnect)
            uniqueTumSummaryError = []
            tuplaTumEquipmentShiftdate = set()
            for diccTUM in equipmentByDayError:
                newTupla = (diccTUM['Equipment_id'], diccTUM['shift_date'])
                if newTupla not in tuplaTumEquipmentShiftdate:
                    tuplaTumEquipmentShiftdate.add(newTupla)
                    uniqueTumSummaryError.append(diccTUM)
            print(uniqueTumSummaryError)
            if(len(uniqueTumSummaryError)>0):
                reconciliadorTumByHour(sqlConnect, uniqueTumSummaryError)
                print('Finalizo la reconciliacion') 
            else:
                flag_is_ok=True 
            print('No existen modificaciones')
        print('Termino el procesamiento, ya no hay registros que editar')
    except Exception as err:
        print(err)

connect_sqlServer =['199.19.74.79', 'sa', 'Model+Mining+Simplefms', 'simplefms_lisbon_mine']
mainReconciliadorTUmByHour(connect_sqlServer)