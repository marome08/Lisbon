import warnings
import pandas as pd
from datetime import datetime, timedelta
import time, datetime
import json
import pyodbc
import pytz
import numpy as np
warnings.simplefilter('ignore')

def searchDistanceTravel(sqlConnect, diccDataTravel):
    returnValue = 0
    distancesList = []
    querySelectDistance = """SELECT  distance_meters FROM travels where destination_location_id=? AND
                             origin_location_id=? AND travel_id not in (?)
                             AND distance_meters not in (1111111,9999999,999999, 888888, -1, 0) and distance_meters>0
                             ORDER BY time_stamp desc"""
    #print('Buscando viajes')
    travelId = diccDataTravel['travelId']
    originId = diccDataTravel['originId']
    destinationId = diccDataTravel['destinationId']
    equipmentId = diccDataTravel['equipmentId']
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        cursor.execute(querySelectDistance, destinationId, originId, travelId)
        resulDistanceList = cursor.fetchall()
        for distance in resulDistanceList:
            #print(distance)
            distancesList.append(int(distance[0]))
    return distancesList

def filterListDistances(distanceList):
    if(len(distanceList)==1):
        return distanceList
    print('Se filtran las distancias')
    q1 = np.percentile(distanceList, 25)
    q3 = np.percentile(distanceList, 75)
    print('Primer cuartil ' + str(q1))
    print('Tercer cuartil ' + str(q3))
    iqr = q3 - q1
    print('IQR = ' + str(iqr))
    limInferior = q1 - 1.5 * iqr
    limSuperior = q3 + 1.5 * iqr
    print(str(limInferior) + ' <= |-----OK-----| <= ' + str(limSuperior))

    distanceFilterList = []
    for x in distanceList:
        if(x >= limInferior and x <= limSuperior):
            distanceFilterList.append(x)
    return distanceFilterList    

def calculatedNewDistance(distanceList):
    distanceAVG = 0
    totalDistanceLen = len(distanceList)
    distanceListFiltered = []
    if(totalDistanceLen >1):
        distanceListFiltered = filterListDistances(distanceList)
    else:
        distanceListFiltered = distanceList
    sumTotalDistance = 0
    print(distanceList)
    print(distanceListFiltered)
    print(totalDistanceLen)
    print(len(distanceListFiltered))
    for distance in distanceListFiltered:
        sumTotalDistance = sumTotalDistance + distance
    print(sumTotalDistance)
    distanceAVG = sumTotalDistance / len(distanceListFiltered)
    print(distanceAVG)
    return distanceAVG

def insertAndUpdateDistance(sqlConnect, diccDistanceTavel):
    newDate = datetime.datetime.now(pytz.utc)
    print("Hora GMT:", newDate)
    queryUpdateTravel = """UPDATE travels SET distance_meters = ? WHERE travel_id=?;"""
    queryInsert = "INSERT INTO obs_distance_travels VALUES (?,?,?,?)"
    queryUpdateObs = "UPDATE obs_distance_travels SET original_distance=?, new_distance=? WHERE travel_id=?"
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        try:
            print('Insertando datos')
            cursor.execute(queryUpdateTravel, diccDistanceTavel['newDistance'], diccDistanceTavel['travelId'])
            cursor.execute(queryInsert, diccDistanceTavel['travelId'], newDate, diccDistanceTavel['originalDistance'], diccDistanceTavel['newDistance'])
            cursor.commit()
            cursor.close()
        except Exception as err:
            print('Error al actualizar la distancia')
            print(err)
            try:
                 cursor.execute(queryUpdateObs, diccDistanceTavel['originalDistance'], 
                                diccDistanceTavel['newDistance'],diccDistanceTavel['travelId'])
                 cursor.commit()
            except:
                cursor.rollback()
            cursor.close()

def mainReconcilerDistanceTravels(sqlConnect, isManual):
    actualDate = datetime.datetime.now()
    startDate = actualDate - datetime.timedelta(days=3)
    startDate = startDate.replace(hour=5, minute=0, second=0, microsecond=0)
    resultAllErrorTravels = None
    print(actualDate)
    print(startDate)
    queryGetAllTravelWithErrors = """SELECT * FROM travels WHERE (distance_meters in (1111111,9999999,999999,888888, -1) OR distance_meters<-1) 
                                    and time_stamp>=?
                                    and time_stamp<=?
                                    order by time_stamp asc"""
    queryGetAllTravelWithErrors2 = """SELECT * FROM travels WHERE (distance_meters in (1111111,9999999,999999,888888, -1) OR distance_meters<-1) 
                                    and time_stamp>='2023-09-02 05:00:00.000'
                                    and time_stamp<='2023-09-11 05:00:00.000'
                                    order by time_stamp asc"""
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+sqlConnect[0]+';DATABASE='+sqlConnect[3]+';UID='+sqlConnect[1]+';PWD='+ sqlConnect[2]) as conexion:
        cursor = conexion.cursor()
        if(isManual):
            print('reconciliar distancias')
            #cursor.execute(queryGetAllTravelWithErros2)
            #resultAllErrorTravels = cursor.fetchall()
        else:
            cursor.execute(queryGetAllTravelWithErrors, startDate, actualDate)
            resultAllErrorTravels = cursor.fetchall()
        print(len(resultAllErrorTravels))
        for travel in resultAllErrorTravels:
            travelId = travel[0]
            originId = travel[2]
            destinationId = travel[1]
            equipmentId = travel[7]
            originalDistance = travel[31]
            diccDataTravel = {'travelId': travelId, 'originId': originId, 
                              'destinationId': destinationId, 'equipmentId':equipmentId}
            #print('Origin: ' + str(originId) + ' , Destination: ' + str(destinationId) + ' , Equipment: ' + str(equipmentId))
            listDistances = searchDistanceTravel(sqlConnect, diccDataTravel)    
            if(len(listDistances)>0):
                newDistance = calculatedNewDistance(listDistances)
                if(newDistance>0):
                    print('Se deben ingresar los datos a la bd')
                    diccNewDistenceByTravel = {'travelId': travelId, 
                                               'originalDistance':originalDistance, 
                                               'newDistance':newDistance}
                    print(diccNewDistenceByTravel)
                    insertAndUpdateDistance(sqlConnect, diccNewDistenceByTravel)
            else:
                print(diccDataTravel)
                diccNewDistenceByTravel = {'travelId': travelId, 
                                            'originalDistance':originalDistance, 
                                           'newDistance':-1}
                insertAndUpdateDistance(sqlConnect, diccNewDistenceByTravel)
            print('-----------------------------------')