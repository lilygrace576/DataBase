## imports etc
############################################################################
import datetime
from influxdb import InfluxDBClient

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.dates as mdates
import matplotlib.colorbar as colorbar
from matplotlib import colormaps
import matplotlib.cm as cm
import pandas as pd
import json
import numpy as np
import datetime as datetime
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import argparse
from scipy import stats
import csv

##
import os
import csv
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import time
import pytz

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'
############################################################################

## user input defs
############################################################################
def startDateIn():
    startDate=input(color.RED + "start date: " + color.END)
    print(startDate)
    return startDate

def endDateIn():
    endDate=input(color.RED + "end date: " + color.END)
    print(endDate)
    return endDate

def rankIn():
    rank1, rank2=input(color.RED + "rank(s): " + color.END).split()
    print(f"{rank1}, {rank2}")
    return rank1, rank2

def dORf():
    want=input(color.RED + "dates or files: " + color.END)
    print(want)
    return want

def hvValIn():
    hvVal=input(color.RED + "hvValues1: " + color.END)
    print(hvVal)
    return hvVal

## 2 inputs for range
def hvCurrIn():
    hvCurr1, hvCurr2=input(color.RED + "hvCurrents1 range: " + color.END).split()
    print(f"{hvCurr1}, {hvCurr2}")
    return hvCurr1, hvCurr2

## 2 inputs for range
def sunAltIn():
    sunAlt1, sunAlt2=input(color.RED + "sun altitude range: " + color.END).split()
    print(f"{sunAlt1}, {sunAlt2}")
    return sunAlt1, sunAlt2

## 2 inputs for range
def moonAltIn():
    moonAlt1, moonAlt2=input(color.RED + "moon altitude: " + color.END).split()
    print(f"{moonAlt1}, {moonAlt2}")
    return moonAlt1, moonAlt2

def opModeIn():
    opMode=input(color.RED + "operation mode: " + color.END)
    print(opMode)
    return opMode

def doorIn():
    door=input(color.RED + "door position: " + color.END)
    print(door)
    return door
############################################################################

## main function
############################################################################
def main():
    ## Local host lines for access
    host = 'localhost'
    port = 8086
    username = 'admin'
    database = 'TDFiles'
    password = 'Ttys@210'

    ## Initialize the InfluxDB client and write the points in batches
    client = InfluxDBClient(host = host, port=port, username=username, password=password)

    ## Switch to the database
    client.switch_database(database)

    ## RankingInfo
    result = client.query(f'SELECT "Filename","ranking","OpMode" FROM "RankingInfo"')

    Filename = []
    Ranking = []
    OpMode = []

    for point in result.get_points():
        Filename.append(point['Filename'])
        Ranking.append(point['ranking'])
        OpMode.append(point['OpMode'])

    data = np.array([Filename, Ranking, OpMode]).T

    ## Create a Ranking info DataFrame
    dfRanking = pd.DataFrame(data, columns=['Filename', 'Ranking', 'OpMode'])

    ## DataInfo
    result = client.query(f'SELECT "hvValues1", "hvcurrents1", "Filename" FROM "DataInfo"')


    hvValues1 = []
    hvcurrents1 = []
    Filename = []

    for point in result.get_points():
        hvValues1.append(point['hvValues1'])
        hvcurrents1.append(point['hvcurrents1'])
        Filename.append(point['Filename'])

    data = np.array([hvValues1, hvcurrents1, Filename]).T

    ## Create a Data info DataFrame 
    dfData = pd.DataFrame(data, columns=['hvValues1', 'hvcurrents1', 'Filename'])

    ## CelestialInfo
    result = client.query(f'SELECT "sunAltitude", "moonAltitude", "Filename" FROM "CelestialInfo"')

    sunAltitude = []
    moonAltitude = []
    Filename = []
    for point in result.get_points():
        sunAltitude.append(point['sunAltitude'])
        moonAltitude.append(point['moonAltitude'])
        Filename.append(point['Filename'])

    data = np.array([sunAltitude, moonAltitude, Filename]).T

    ## Create a Celestial info DataFrame
    dfCelestial = pd.DataFrame(data, columns=['sunAltitude', 'moonAltitude', 'Filename'])

    ## Merge the DataFrames on 'Filename'
    dfMerged = pd.merge(dfRanking, dfData, on='Filename')
    dfMerged = pd.merge(dfMerged, dfCelestial, on='Filename')

    dfMerged['Ranking'] = pd.to_numeric(dfMerged['Ranking'], errors='coerce')
    dfMerged['Ranking'] = dfMerged['Ranking'].fillna(0).astype(int) 
    dfMerged['OpMode'] = pd.to_numeric(dfMerged['OpMode'], errors='coerce')
    dfMerged['OpMode'] = dfMerged['OpMode'].fillna(0).astype(int) 
    dfMerged['hvValues1'] = dfMerged['hvValues1'].astype(float)
    dfMerged['hvcurrents1'] = dfMerged['hvcurrents1'].astype(float)
    dfMerged['sunAltitude'] = dfMerged['sunAltitude'].astype(float)
    dfMerged['moonAltitude'] = dfMerged['moonAltitude'].astype(float)

    ## only include Rank 8 data
    # dfFiltered = dfMerged[(dfMerged['Ranking'] == 8)]

    dfFiltered = dfMerged
    # print(dfFiltered)

    ## extract dates from filenames
    date = dfFiltered['Filename'].str.extract(
        r'(\d{4}-\d{2}-\d{2}|\d{8})'
    )[0]
    ## create new column in df for dates
    dfFiltered['Date'] = (
        date
        .str.replace('-', '', regex=True)
        .astype(int)
    )
    # print(dfFiltered['Date'])
    ## some files output as "20241213T00:00:00.00_FolderEmpty" so this extracts dates in either format yyyy-mm-dd or yyyymmdd

    ## create a new column with the data from the filename column so that it is yyyy-mm-dd
    # dfFiltered['Date'] = (pd.to_datetime(dfFiltered['Filename'].str[12:22])).astype(str)
    # dfFiltered['Date'] = (dfFiltered['Date'].str.replace('-','')).astype(int)

    ## get user input start/end dates
    startDate=startDateIn()
    endDate=endDateIn()
    ## check for user input
    if startDate != "n":
        dfFiltered = dfFiltered[dfFiltered['Date'] >= int(startDate)]
    else:
        dfFiltered = dfFiltered[dfFiltered['Date'] >= 20250101]
    if endDate != "n":
        dfFiltered = dfFiltered[dfFiltered['Date'] <= int(endDate)]
    else:
        dfFiltered = dfFiltered[dfFiltered['Date'] <= 20251231]

    ## set opmodes = all opmodes of files in dfFiltered
    opmodes = dfFiltered['OpMode']
    ## set vars = sun and moon alts
    absSunAlt = abs(dfFiltered['sunAltitude'])
    absMoonAlt = abs(dfFiltered['moonAltitude'])

    ## write output txt file
    with open('dbQuery.txt', mode='w', newline='') as file:
        ## write header
        file.write("Trinity Demonstrator Database Output - Queries \n")
        file.write("")
        file.write(f"Period of time : {dfFiltered['Date'].min()} to {dfFiltered['Date'].max()} \n")

            ## check for user input
        rank1, rank2=rankIn()
        if rank1 != "n" and rank2 != "n":
            file.write(f"Ranks: {rank1}, {rank2} \n")
            dfFiltered = dfFiltered[dfFiltered['Ranking'].isin([int(rank1), int(rank2)])]
        elif rank1 != "n" and rank2 == "n":
            file.write(f"Rank: {rank1} \n")
            dfFiltered = dfFiltered[dfFiltered['Ranking'] == int(rank1)]
        else: 
            dfFiltered = dfFiltered
            # print(dfFiltered)

        ## check user input for if we want indiv files or dates
        datatype=dORf()
        ## want indiv file data
        if datatype == "files":
            ## check user input for specific hvvalue
            hvVal=hvValIn()
            if hvVal != "n":
                file.write(f"hvValues = {hvVal} \n")
                ## set hvValR = all hvvalues of files in dfFiltered
                hvValR = dfFiltered['hvValues1']
                ## round those values to integer
                hvValR = round(hvValR)
                ## only include files with that hvvalue
                dfFiltered = dfFiltered[hvValR == float(hvVal)]

            ## check user input for specific hvcurrent (range)
            hvCurr1, hvCurr2=hvCurrIn()
            if hvCurr1 != "n" and hvCurr2 != "n":
                file.write(f"hvCurrent range = {hvCurr1} - {hvCurr2} \n")
                dfFiltered = dfFiltered[dfFiltered['hvcurrents1'] >= float(hvCurr1)]
                dfFiltered = dfFiltered[dfFiltered['hvcurrents1'] <= float(hvCurr2)]
            elif hvCurr1 != "n" and hvCurr2 == "n":
                file.write(f"hvCurrent: > {hvCurr1} \n")
                dfFiltered = dfFiltered[dfFiltered['hvcurrents1'] >= float(hvCurr1)]
            elif hvCurr2 != "n" and hvCurr1 == "n":
                file.write(f"hvCurrent: < {hvCurr2} \n")
                dfFiltered = dfFiltered[dfFiltered['hvcurrents1'] <= float(hvCurr2)]

            ## check user input for specific sunAltitude (range)
            sunAlt1, sunAlt2=sunAltIn()
            if sunAlt1 != "n" and sunAlt2 != "n":
                file.write(f"sun altitude range = {sunAlt1} - {sunAlt2} \n")
                dfFiltered = dfFiltered.loc[absSunAlt >= abs(int(sunAlt1))]
                dfFiltered = dfFiltered.loc[absSunAlt <= abs(int(sunAlt2))]
            elif sunAlt1 != "n" and sunAlt2 == "n":
                file.write(f"sun altitude: > {sunAlt1} \n")
                dfFiltered = dfFiltered.loc[absSunAlt >= abs(int(sunAlt1))]
            elif sunAlt2 != "n" and sunAlt1 == "n":
                file.write(f"sun altitude: < {sunAlt2} \n")
                dfFiltered = dfFiltered.loc[absSunAlt <= abs(int(sunAlt2))]
               
            ## check user input for specific moonAltitude
            moonAlt1, moonAlt2=moonAltIn()
            if moonAlt1 != "n" and moonAlt2 != "n":
                file.write(f"moon altitude range = {moonAlt1} - {moonAlt2} \n")
                dfFiltered = dfFiltered.loc[absMoonAlt >= abs(int(moonAlt1))]
                dfFiltered = dfFiltered.loc[absMoonAlt <= abs(int(moonAlt2))]
            elif moonAlt1 != "n" and moonAlt2 == "n":
                file.write(f"moon altitude: > {moonAlt1} \n")
                dfFiltered = dfFiltered.loc[absMoonAlt >= abs(int(moonAlt1))]
            elif moonAlt2 != "n" and moonAlt1 == "n":
                file.write(f"moon altitude: < {moonAlt2} \n")
                dfFiltered = dfFiltered.loc[absMoonAlt <= abs(int(moonAlt2))]

            ## check user input for specific opmode
            op=opModeIn()
            if op != "n":
                file.write(f"operation mode = {op} \n")
                ## only include files with that opmode
                dfFiltered = dfFiltered[opmodes == int(op)]
            ## write total days, files, and hours data to txt
            file.write(f"Total Days: {len(dfFiltered['Date'].unique())} days \n")
            file.write(f"Total Files: {len(dfFiltered['Filename'].unique())} files \n")
            file.write(f"Total Hours Data: {(len(dfFiltered['Filename'].unique()) * 97) / 3600:.1f} hours \n")
            file.write("listing files \n")
            file.write("\n")
            file.write("File: \n")
            unique_files = dfFiltered['Filename'].unique()
            ## write filename for each file to txt
            for f in unique_files:
                file.write(f"{f}, ")
                ## if opmode not specified, also write file opmode to txt
                if op == "":
                    file.write(f"{op} \n")
                else:
                    file.write("\n")

        ## want dates data
        elif datatype == "dates":
            ## find open, closed, and extmoon data
            open_hours = 0
            extmoon_hours = 0
            closed_hours = 0
            for i in opmodes:
                ## open data
                if i == 1:
                    total_open_data = dfFiltered[dfFiltered['OpMode'] == i]
                    if len(total_open_data) != 0:          
                        open_files = len(total_open_data)
                        open_hours = (open_files * 97) / 3600
                    else:
                        open_hours = 0
                ## extmoon data
                elif i == 2:
                    total_extmoon_data = dfFiltered[dfFiltered['OpMode'] == i]
                    if len(total_extmoon_data) != 0:          
                        total_extmoon_files = len(total_extmoon_data)
                        extmoon_hours = (total_extmoon_files * 97) / 3600
                    else:
                        extmoon_hours = 0
                ## closed data
                elif i ==3:
                    total_closed_data = dfFiltered[dfFiltered['OpMode'] == i]
                    if len(total_closed_data) != 0:          
                        closed_files = len(total_closed_data)
                        closed_hours = (closed_files * 97) / 3600
                    else:
                        closed_hours = 0

            ## write total days, files, and hours data to txt
            file.write(f"Total Days: {len(dfFiltered['Date'].unique())} days \n")
            file.write(f"Total Files: {len(dfFiltered['Filename'].unique())} files \n")
            file.write(f"Total Hours Data: {(len(dfFiltered['Filename'].unique()) * 97) / 3600:.1f} hours \n")
            ## write total open, closed, ext moon hours data to txt
            file.write(f"Door Open Hours Data: {open_hours:.1f} hours \n")
            file.write(f"Extended Moon Hours Data: {extmoon_hours:.1f} hours \n")
            file.write(f"Door Closed Hours Data: {closed_hours:.1f} hours \n")
            file.write("listing dates \n")
            
            ## get the unique values in the Date column
            unique_dates = dfFiltered['Date'].unique()

            ## check user input for specific opmode
            door=doorIn()
            if door == "n":
                file.write("\n")
                file.write("Date, Files, Hours, Door Position \n")
            else: 
                file.write(f"door position = {door} \n")
                file.write("\n")
                file.write("Date, Files, Hours \n")


            ## indiv date data
            for date in unique_dates:
                ## all files for each night
                day_data = dfFiltered[dfFiltered['Date'] == date]
                ## number of files for each night
                day_files = len(day_data)
                ## number of hours for each night
                day_hours_data = (day_files * 97) / 3600

                ## open, closed, extmoon data for each date
                day_open_data = day_data[day_data['OpMode'] == 1]
                day_open_files = len(day_open_data)

                day_closed_data = day_data[day_data['OpMode'] == 3]
                day_closed_files = len(day_closed_data)

                day_extmoon_data = day_data[day_data['OpMode'] == 2]
                day_extmoon_files = len(day_extmoon_data)

                ## door position for each date
                ## any extmoon files = e
                if day_extmoon_files != 0:
                    door_position = "e"
                ## no extmoon files but any open files = o
                elif day_open_files != 0:
                    door_position = "o"
                ## no extmoon or open files, only closed files = c
                else:
                    door_position = "c"

                ## check which door position wanted
                ## write that door position's dates data to txt
                if door == "o":
                    if door_position == "o":
                        file.write(f"{date}, {day_files}, {day_hours_data:.2f} \n")
                elif door == "e":
                    if door_position == "e":
                        file.write(f"{date}, {day_files}, {day_hours_data:.2f} \n")
                elif door == "c":
                    if door_position == "c":
                        file.write(f"{date}, {day_files}, {day_hours_data:.2f} \n")
                ## if no door position specified, include door position for each date in txt
                elif door == "n":
                    file.write(f"{date}, {day_files}, {day_hours_data:.2f}, {door_position} \n")
        
    print("Query Files Data written to dbQuery.txt")
    

if __name__ == "__main__":
    main()
############################################################################
       

