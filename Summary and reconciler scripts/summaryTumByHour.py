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


def tranformacionTumByHour(sqlConnect):
    listTUM2 = []
    queryGetDateLastRecordTUMByHour = """SELECT MAX(shift_date) from tum_by_hours where equipment_id=?"""
    queryGetEquipments = """SELECT equipment_id from equipment where equipment_active=1 and equipment_id not in (539)"""
    queryGetstates = """SELECT * FROM view_hist_states where shift_date > ? and shift_date <= ? 
                        and equipment_id=? and status=0 order by start_date asc;"""
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        cursor.execute(queryGetEquipments)
        resultEquipments = cursor.fetchall()
        print(resultEquipments)
        for equipment_id in resultEquipments:
            print('---------------------------------------------------------------------------------------------------')
            print(equipment_id[0])
            cursor.execute(queryGetDateLastRecordTUMByHour, equipment_id[0])
            resultDatebyEquipment = cursor.fetchone()
            startDateTum = datetime.datetime.strptime('2024-07-01', '%Y-%m-%d')
            if(resultDatebyEquipment[0]!=None):
                print('entro Al if')
                startDateTum = resultDatebyEquipment[0]
                print(startDateTum)
            endDate = None
            actualDatetimeSimberiAux = datetime.datetime.now(pytz.timezone('America/Denver'))
            #actualDatetimeSimberiAux = actualDatetimeSimberiAux - datetime.timedelta(hours=1)
            print(f"Datetime Pacific/Port_Moresby es: {actualDatetimeSimberiAux}")
            if(actualDatetimeSimberiAux.hour < 6):
                print('no a terminado el turno')
                #return 
            auxtime = actualDatetimeSimberiAux - datetime.timedelta(days=1)
            print(auxtime)
            endDate = auxtime.strftime('%Y-%m-%d')
            startDateTum = startDateTum.strftime('%Y-%m-%d')
            print(type(endDate))
            print(type(startDateTum))
            if(endDate == startDateTum):
                print('Misma fecha')
                return
            print(startDateTum)
            print(endDate)
            print(equipment_id[0])
            cursor.execute(queryGetstates, startDateTum, endDate, equipment_id[0])
            resultGetstates = cursor.fetchall()
            print(resultGetstates)
            if(len(resultGetstates)<=0):
                print('Sin registros')
                
            for state in resultGetstates:
                statesCorrect = separacionStateByHour(state)
                if(len(statesCorrect)>0):
                    for newState in statesCorrect:
                        if(newState['seconds'] != 0 ):
                            listTUM2.append(newState)
    return listTUM2

def insertTumByHour(sqlConnect, tumByHourList):
    queryInsertTumByHour = """INSERT INTO tum_by_hours VALUES (?,?,?,?,?,?,?,?,?);"""
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
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

def mainTumByHour(sqlConnect):
    tumHourList = tranformacionTumByHour(sqlConnect)
    if(tumHourList != None):
        print('Lista con datos')
        print(tumHourList)
        insertTumByHour(sqlConnect,tumHourList)
    print('Fin ejecucion')

connect_sqlServer =['199.19.74.79', 'sa', 'Model+Mining+Simplefms', 'simplefms_lisbon_mine']
mainTumByHour(connect_sqlServer)