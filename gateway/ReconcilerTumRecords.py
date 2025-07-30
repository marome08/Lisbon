import warnings
import pandas as pd
import datetime
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
        newDate = startDate.replace(hour=newHour, minute=0)
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
                endDateAux = startDate.replace(hour=valor + 1 , minute=0)
            elif indice == len(hourList) - 1:
                #print("Estás en el último valor:", valor)
                startDateAux = startDate.replace(hour=valor, minute=0)
                endDateAux = endDate
            else:
                #print("Estás en un valor intermedio:", valor)
                startDateAux = startDate.replace(hour=valor, minute=0)
                endDateAux = startDate.replace(hour=valor + 1, minute=0)
            
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
            endDateAux = auxNewDayFull.replace(hour=00 , minute=0)
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
                    endDateAux = startDate.replace(hour=valor + 1 , minute=0)
                elif indice == len(hourList) - 1:
                    startDateAux = startDate.replace(hour=valor, minute=0)
                    endDateAux = auxNewDayFull
                else:
                    startDateAux = startDate.replace(hour=valor, minute=0)
                    endDateAux = startDate.replace(hour=valor + 1, minute=0)
                
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
                    endDateAux = auxNewDayFull.replace(hour=valor + 1 , minute=0)
            elif indice == len(hourList2) - 1:
                startDateAux = auxNewDayFull.replace(hour=valor, minute=0)
                endDateAux = endDate
            else:
                startDateAux = auxNewDayFull.replace(hour=valor, minute=0)
                endDateAux = auxNewDayFull.replace(hour=valor + 1, minute=0)
                
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

def tranformacionTumByHour(cursor, equipmentId, date):
    listTUM2 = []
    
    queryGetstates = """SELECT * FROM view_hist_states where shift_date = ?
                        and equipment_id=? order by start_date asc;"""
    cursor.execute(queryGetstates, date, equipmentId)
    resultGetstates = cursor.fetchall()
    if(len(resultGetstates)<=0):
        return []
    for states in resultGetstates:
        statesCorrect = separacionStateByHour(states)
        if(len(statesCorrect)>0):
            for newState in statesCorrect:
                if(newState['seconds'] != 0 ):
                    listTUM2.append(newState)
    return listTUM2

def insertTumByHour(cursor, tumByHourList, equipmentId, dateShift):
    print('Entro al insert')
    queryInsertTumByHour = """INSERT INTO tum_by_hours VALUES (?,?,?,?,?,?,?,?,?);"""
    queryDeleteTumByHour = """  DELETE FROM tum_by_hours WHERE shift_date = ? and equipment_id = ?"""
    try:
        cursor.execute(queryDeleteTumByHour, dateShift, equipmentId)
        for tumByHour in tumByHourList:
            shift = 'Night'
            if(tumByHour['hourDate'] >= 5 and tumByHour['hourDate'] < 17):
                shift = 'Day'
            #print(tumByHour)
            try:
                cursor.execute(queryInsertTumByHour, tumByHour['state'], tumByHour['equipmentId'], tumByHour['reasonId'],
                                tumByHour['startDate'], tumByHour['endDate'], tumByHour['shiftDate'], shift, tumByHour['hourDate'],
                                tumByHour['seconds'])
                print('Insertado')
            except pyodbc.Error as db_err:
                    print('Error al insertar en la base de datos:')
                    print(db_err)
                    break
        cursor.commit()
        return 1
    except Exception as err:
        print('Error desconocido:')
        print(err)
        cursor.rollback()
        return 0

def reconcilerRunIdleParked(cursor, reportTimeTruckId):
    tumRipList = []
    resultEventTum = None
    equipment_id = None
    startDate = None
    endDate = None
    querySelectInfoReport = """SELECT reports_time_trucks_id ,equipment_id, start_date, end_date, duration
                                  from view_strip_state where  reports_time_trucks_id=?;"""
    
    queryGetStatusCase1 = """select * from view_hist_states where equipment_id=? 
                                AND start_date<=? and end_date>=? order by start_date asc;"""
    queryGetStatusCase2 = """select * from view_hist_states where equipment_id=?
                                AND start_date<=? and end_date>=?
                                UNION
                                select * from view_hist_states where equipment_id=?
                                AND start_date<=? and end_date>=?
                                order by start_date asc"""
    queryGetStatusCase3 = """select * from view_hist_states where equipment_id=? 
                                AND start_date<=? and end_date>=?
                                UNION
                                select * from view_hist_states where equipment_id=? 
                                AND start_date>=? and end_date<=?
                                UNION
                                select * from view_hist_states where equipment_id=? 
                                AND start_date<=? and end_date>=?
                                order by start_date asc
                                """
    try:
        cursor.execute(querySelectInfoReport, reportTimeTruckId)
        resultRipEvents =  cursor.fetchone()
        equipment_id = int(resultRipEvents[1])
        startDate = resultRipEvents[2]
        endDate = resultRipEvents[3]
        duration = resultRipEvents[4] 
        #print(resultRipEvents)
        cursor.execute(queryGetStatusCase1, equipment_id, startDate, endDate)
        resultEventTum = cursor.fetchall()
        if(len(resultEventTum)==0):
            cursor.execute(queryGetStatusCase3, equipment_id, startDate, startDate, equipment_id, startDate, endDate,
                           equipment_id, endDate, endDate)
            resultEventTum = cursor.fetchall()
            if(len(resultEventTum)==0):
                cursor.execute(queryGetStatusCase2, equipment_id, startDate, startDate, equipment_id,
                               endDate, endDate)
                resultEventTum = cursor.fetchall()
        if(len(resultEventTum)==1):
            auxDiccRipTum = {'reports_time_trucks_id':reportTimeTruckId, 'state_id':resultEventTum[0][0],
                        'start_date':startDate,'end_date':endDate, 'Duration':duration}
            tumRipList.append(auxDiccRipTum)
        elif(len(resultEventTum)==2):
            print('Aqui 2')
            
            auxDuration1 = resultEventTum[0][11] - startDate
            durationStatus1 =  auxDuration1.total_seconds()
            auxDuration2 =  endDate - resultEventTum[1][10]
            durationStatus2 = auxDuration2.total_seconds()
            auxDiccRipTum1 = {'reports_time_trucks_id':reportTimeTruckId, 'state_id':resultEventTum[0][0],
                        'start_date':startDate,'end_date':resultEventTum[0][11],
                        'Duration':durationStatus1}
            tumRipList.append(auxDiccRipTum1)
            auxDiccRipTum2 = {'reports_time_trucks_id':reportTimeTruckId, 'state_id':resultEventTum[1][0],
                        'start_date':resultEventTum[1][10],'end_date':endDate,
                        'Duration':durationStatus2}
            tumRipList.append(auxDiccRipTum2)
        elif(len(resultEventTum)==0):
            print('ERROR')
        else:
            print('son mas de dos')
            for i in range(len(resultEventTum)):
               
                if i == 0:
                    auxDuration1 = resultEventTum[i][11] - startDate
                    durationStatus1 = auxDuration1.total_seconds()
                    aux_dict1 = {'reports_time_trucks_id':reportTimeTruckId, 
                                'state_id':resultEventTum[i][0],
                                'start_date':startDate,'end_date':resultEventTum[i][11],
                                'Duration':durationStatus1
                                }
                    tumRipList.append(aux_dict1)
                elif i == len(resultEventTum) - 1:
                    auxDuration2 = endDate - resultEventTum[i][10]
                    durationStatus2 = auxDuration2.total_seconds()
                    aux_dict2 = {'reports_time_trucks_id':reportTimeTruckId, 
                                'state_id':resultEventTum[i][0],'start_date':resultEventTum[i][10],
                                'end_date':endDate,'Duration':durationStatus2
                                }
                    tumRipList.append(aux_dict2)
                else:
                    auxDuration = resultEventTum[i][11] - resultEventTum[i][10]
                    durationStatus = auxDuration.total_seconds()
                    aux_dict_bucle = {'reports_time_trucks_id':reportTimeTruckId, 
                                    'state_id':resultEventTum[i][0],
                                    'start_date':resultEventTum[i][10],'end_date':resultEventTum[i][11],
                                    'Duration':durationStatus
                                    }
                    tumRipList.append(aux_dict_bucle)
        return tumRipList
    except Exception as err:
        print('error')
        print(reportTimeTruckId)
        print(startDate)
        print(endDate)
        print(err)
        return []

def insertReconcilerRecords(cursor, ripTumList, equipmentId, shiftDate):
    queryInsertRipTum = """INSERT INTO report_time_trucks_tum VALUES(?,?,?,?,?);"""
    queryDeleteRipTum = """delete from report_time_trucks_tum where reports_time_trucks_id=?;"""
    print(equipmentId)
    print(shiftDate)
    print(ripTumList)
    try:
        for x in ripTumList:
            cursor.execute(queryDeleteRipTum, x['reports_time_trucks_id'])
        cursor.commit()
        for x in ripTumList:
            cursor.execute(queryInsertRipTum, x['reports_time_trucks_id'], x['state_id'],
                            x['start_date'], x['end_date'], int(x['Duration']))
        cursor.commit()
        return 1
    except pyodbc.Error as db_err:
        print(db_err)
        cursor.rollback()
        return 0

def mainReconciliadorTUmByHourAndRip(sqlConnect):
    try:
        print('Main Reconciliador')
        queryGetTumActions = """select *, CONVERT(DATE, DATEADD(hh,-5, reconciler_date)) as shift_date
                                from reconciler_states_actions WHERE status='TBC';"""
        queryGetStripState = """SELECT * FROM view_strip_state 
                                where shift_date=? and equipment_id=?;"""
        with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
            cursor = conexion.cursor()
            cursor.execute(queryGetTumActions)
            totalTumActions = cursor.fetchall()
            simberiTz = pytz.timezone('Australia/Brisbane')
            utcNow = datetime.datetime.utcnow()
            simberiNow = utcNow.replace(tzinfo=pytz.utc).astimezone(simberiTz)
            simberiNowFormatBd = simberiNow.strftime('%Y-%m-%d %H:%M:%S')
            print("Hora en Brisbane:", simberiNowFormatBd)
            if(len(totalTumActions)>0):
                queryUpdateTable = """UPDATE reconciler_states_actions SET status=?, 
                                      ending_date=? where reconciler_states_id=?"""
                for action in totalTumActions:
                    print(action)
                    updateStatusMessage = None
                    if(action[1]=='tum_by_hours'):
                        print('Es tum by hour')
                        auxData  = tranformacionTumByHour(cursor, action[5], action[7])
                        
                        if(len(auxData)>0):
                            resultEjecution = insertTumByHour(cursor, auxData, action[5], action[7])
                            if(resultEjecution == 1):
                                updateStatusMessage = 'Completed'
                            else:
                                updateStatusMessage = 'Error'
                        else:
                            print('Lista vacia, posible error')
                            updateStatusMessage = 'Empty list'
                        cursor.execute(queryUpdateTable, updateStatusMessage, simberiNowFormatBd, action[0])
                    elif(action[1]=='report_time_trucks_tum'):
                        print('Es tabla run, idle, parked')
                        cursor.execute(queryGetStripState, action[7], action[5])
                        resultRIP = cursor.fetchall()
                        ripTumList = []
                        lenResultRIP = len(resultRIP)
                        if(lenResultRIP>0):
                            print('equipo tiene gps')
                            #print(lenResultRIP)
                            for reportTimeTruck in resultRIP:
                                #print(reportTimeTruck)
                                listRipTum = reconcilerRunIdleParked(cursor, reportTimeTruck[0])
                                if(len(listRipTum)>0):
                                    for recordListRipTum in listRipTum:
                                        ripTumList.append(recordListRipTum)
                            resultInsert = insertReconcilerRecords(cursor, ripTumList, action[5], action[7])
                            if(resultInsert == 1):
                                updateStatusMessage='Completed'
                            else:
                                updateStatusMessage='Error'
                        else:
                            print('equipo no tiene gps')  
                            updateStatusMessage = 'Equipment without GPS' 
                        cursor.execute(queryUpdateTable, updateStatusMessage, simberiNowFormatBd, action[0])                     
                    else:
                        print('Otros')
            else:
                print('No existen nuevos registros')
        return 0
    except Exception as err:
        print(err)
        return 3

#connect_sqlServer =['LAPTOP-ITKSBB0N', 'sa', '3011', 'simberi_developer2']
#mainReconciliadorTUmByHour(connect_sqlServer)