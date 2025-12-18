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


# user input defs
############################################################################
def startDateIn():
    startDate=input(color.RED + "start date:" + color.END)
    print(startDate)
    return startDate

def endDateIn():
    endDate=input(color.RED + "end date:" + color.END)
    print(endDate)
    return endDate

def dORf():
    want=input(color.RED + "dates or files:" + color.END)
    print(want)
    return(want)

def hvValIn():
    hvVal=input(color.RED + "hvValues1:" + color.END)
    print(hvVal)
    return hvVal

def hvCurrIn():
    hvCurr=input(color.RED + "hvCurrents1:" + color.END)
    print(hvCurr)
    return hvCurr

def sunAltIn():
    sunAlt=input(color.RED + "sun altitude:" + color.END)
    print(sunAlt)
    return sunAlt

def moonAltIn():
    moonAlt=input(color.RED + "moon altitude:" + color.END)
    print(moonAlt)
    return moonAlt

def opModeIn():
    opMode=input(color.RED + "operation mode:" + color.END)
    print(opMode)
    return opMode

def doorIn():
    door=input(color.RED + "door position:" + color.END)
    print(door)
    return door
############################################################################


## functional defs
############################################################################
## get total open/closed/ext hours for entire output
## args are opmode and dfFiltered ?
def getOpHours(o, df):
    open_hours = 0
    extmoon_hours = 0
    closed_hours = 0
    weird_hours = 0
    intrigs_hours = 0

    for i in o:
        # open hours
        if i == 1:
            total_open_data = df[df['OpMode'] == i]
            if len(total_open_data) != 0:          
                open_files = len(total_open_data)
                open_hours = (open_files * 97) / 3600
            else:
                open_hours = 0
        # ext moon hours
        elif i == 2:
            total_extmoon_data = df[df['OpMode'] == i]
            if len(total_extmoon_data) != 0:          
                total_extmoon_files = len(total_extmoon_data)
                extmoon_hours = (total_extmoon_files * 97) / 3600
            else:
                extmoon_hours = 0
        # closed hours
        elif i ==3:
            total_closed_data = df[df['OpMode'] == i]
            if len(total_closed_data) != 0:          
                closed_files = len(total_closed_data)
                closed_hours = (closed_files * 97) / 3600
            else:
                closed_hours = 0
        # intrigs hours        
        elif i == 0:
            total_intrigs_data = df[df['OpMode'] == i]
            if len(total_intrigs_data) != 0:          
                intrigs_files = len(total_intrigs_data)
                intrigs_hours = (intrigs_files * 97) / 3600
            else:
                intrigs_hours = 0
        # weird data hours
        elif i == 6:
            total_weird_data = df[df['OpMode'] == i]
            if len(total_weird_data) != 0:          
                weird_files = len(total_weird_data)
                weird_hours = (weird_files * 97) / 3600
            else:
                weird_hours = 0
    return open_hours, closed_hours, extmoon_hours, intrigs_hours, weird_hours

# get door position for each night
## args are unique_dates and dfFiltered ?
def nightDoorPos(dates, df):
    for date in dates:
        ## df with all files for each night
        day_data = df[df['Date'] == date]
        ## total number files and hours for each night
        night_files = len(day_data)
        night_hours_data = (night_files * 97) / 3600
        ## door open data for each night
        door_open_data = day_data[day_data['OpMode'] == 1]
        door_open_files = len(door_open_data)
        night_open_hours = (door_open_files * 97) / 3600
        ## door closed data for each night
        door_closed_data = day_data[day_data['OpMode'] == 3]
        door_closed_files = len(door_closed_data)
        night_closed_hours = (door_closed_files * 97) / 3600
        ## ext moon data for each night
        extmoon_data = day_data[day_data['OpMode'] == 2]
        extmoon_files = len(extmoon_data)
        night_extmoon_hours = (extmoon_files * 97) / 3600
        ## find door position based on file types
        if extmoon_files != 0:
            door_position = "e"
        ## if no ext moon files but any door open files -> night = door open
        elif door_open_files != 0:
            door_position = "o"
        ## if no ext moon or door open files (only door closed files) -> night = door closed
        else:
            door_position = "c"
    return door_position, night_files, night_hours_data, night_open_hours, night_closed_hours, night_extmoon_hours
############################################################################


## main function
############################################################################
def main():
    # Local host lines for access
    host = 'localhost'
    port = 8086
    username = 'admin'
    database = 'TDFiles'
    password = 'Ttys@210'

    # Initialize the InfluxDB client and write the points in batches
    client = InfluxDBClient(host = host, port=port, username=username, password=password)

    # Switch to the database
    client.switch_database(database)

    result = client.query(f'SELECT "Filename","ranking","OpMode" FROM "RankingInfo"')

    Filename = []
    Ranking = []
    OpMode = []

    for point in result.get_points():
        Filename.append(point['Filename'])
        Ranking.append(point['ranking'])
        OpMode.append(point['OpMode'])

    data = np.array([Filename, Ranking, OpMode]).T

    # Create a Ranking info DataFrame
    dfRanking = pd.DataFrame(data, columns=['Filename', 'Ranking', 'OpMode'])

    result = client.query(f'SELECT "hvValues1", "hvcurrents1", "Filename" FROM "DataInfo"')

    hvValues1 = []
    hvcurrents1 = []
    Filename = []

    for point in result.get_points():
        hvValues1.append(point['hvValues1'])
        hvcurrents1.append(point['hvcurrents1'])
        Filename.append(point['Filename'])

    data = np.array([hvValues1, hvcurrents1, Filename]).T
    # Create a Data info DataFrame 
    dfData = pd.DataFrame(data, columns=['hvValues1', 'hvcurrents1', 'Filename'])

    result = client.query(f'SELECT "sunAltitude", "moonAltitude", "Filename" FROM "CelestialInfo"')

    sunAltitude = []
    moonAltitude = []
    Filename = []
    for point in result.get_points():
        sunAltitude.append(point['sunAltitude'])
        moonAltitude.append(point['moonAltitude'])
        Filename.append(point['Filename'])

    data = np.array([sunAltitude, moonAltitude, Filename]).T
    # Create a Celestial info DataFrame
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

    ## filter the Dataframe to only include rank 8 data
    dfFiltered = dfMerged[(dfMerged['Ranking'] == 8)]

    # create a new column with the data from the filename column so that it is yyyy-mm-dd
    dfFiltered['Date'] = (pd.to_datetime(dfFiltered['Filename'].str[12:22])).astype(str)
    dfFiltered['Date'] = (dfFiltered['Date'].str.replace('-','')).astype(int)

    ## set start/end dates = start/end dates input defs
    startDate=startDateIn()
    endDate=endDateIn()
    ## check for start/end date user input
    if startDate != "":
        dfFiltered = dfFiltered[dfFiltered['Date'] >= startDate]
    else:
        dfFiltered = dfFiltered[dfFiltered['Date'] >= 20250101]
    if endDate != "":
        dfFiltered = dfFiltered[dfFiltered['Date'] <= endDate]
    else:
        dfFiltered = dfFiltered[dfFiltered['Date'] <= 20251231]
    
    ## set opmode = all opmodes in dfFiltered
    opmodes = dfFiltered['OpMode']


    ## write output txt file
    ############################################################################
    with open('rank8Query.txt', mode='w', newline='') as file:
        # write header
        file.write("Trinity Demonstrator Database Output - Rank 8 Queries \n")
        file.write("")
        file.write(f"Period of time : {dfFiltered['Date'].min()} to {dfFiltered['Date'].max()} \n")

        # determine if we want indiv files or whole dates listed in output txt
        datatype=dORf()
        # want indiv files
        if datatype == "files":
            file.write("listing files \n")
            # check if we want a specific op mode from user input
            op=opModeIn()
            if op != "":
                file.write(f"operation mode = {op} \n")
                # reduce df to only include data with that opmode
                dfFiltered = dfFiltered[opmodes == op]
            # check if want a specific hvValue from user input
            hvVal=hvValIn()
            if hvVal != "":
                file.write(f"hvValues = {hvVal} \n")
                dfFiltered = dfFiltered[dfFiltered['hvValues1'] == hvVal]
            # check if want a specific hvCurrent from user input
            hvCurr=hvCurrIn()
            if hvCurr != "":
                file.write(f"hvCurrents = {hvCurr} \n")
                dfFiltered = dfFiltered[dfFiltered['hvCurrents1'] == hvCurr]
            # check if want a specific sun altitude from user input
            sunAlt=sunAltIn()
            if sunAlt != "":
                file.write(f"sun altitude = {sunAlt} \n")
                dfFiltered = dfFiltered[dfFiltered['sunAltitude'] == sunAlt]
            # check if want a specific moon altitude from user input
            moonAlt=moonAltIn()
            if moonAlt != "":
                file.write(f"moon altitude = {moonAlt} \n")
                dfFiltered = dfFiltered[dfFiltered['moonAltitude'] == moonAlt]

            # write total data to output txt 
            file.write(f"Total Days: {len(dfFiltered['Date'].unique())} days \n")
            file.write(f"Total Files: {len(dfFiltered['Filename'].unique())} files \n")
            file.write(f"Total Hours: {(len(dfFiltered['Filename'].unique()) * 97) / 3600:.1f} hours \n")
            file.write("File: \n")
            # write each filename to ouput txt
            unique_files = dfFiltered['Filename'].unique()
            for f in unique_files:
                file.write(f"{f} \n")
        # want data for each night
        if datatype == "dates":
            open_hours = 0
            extmoon_hours = 0
            closed_hours = 0
            weird_hours = 0
            intrigs_hours = 0
            for i in opmodes:
            # open hours
                if i == 1:
                    total_open_data = df[df['OpMode'] == i]
                    if len(total_open_data) != 0:          
                        open_files = len(total_open_data)
                        open_hours = (open_files * 97) / 3600
                    else:
                        open_hours = 0
                # ext moon hours
                elif i == 2:
                    total_extmoon_data = df[df['OpMode'] == i]
                    if len(total_extmoon_data) != 0:          
                        total_extmoon_files = len(total_extmoon_data)
                        extmoon_hours = (total_extmoon_files * 97) / 3600
                    else:
                        extmoon_hours = 0
                # closed hours
                elif i ==3:
                    total_closed_data = df[df['OpMode'] == i]
                    if len(total_closed_data) != 0:          
                        closed_files = len(total_closed_data)
                        closed_hours = (closed_files * 97) / 3600
                    else:
                        closed_hours = 0
                # intrigs hours        
                elif i == 0:
                    total_intrigs_data = df[df['OpMode'] == i]
                    if len(total_intrigs_data) != 0:          
                        intrigs_files = len(total_intrigs_data)
                        intrigs_hours = (intrigs_files * 97) / 3600
                    else:
                        intrigs_hours = 0
                # weird data hours
                elif i == 6:
                    total_weird_data = df[df['OpMode'] == i]
                    if len(total_weird_data) != 0:          
                        weird_files = len(total_weird_data)
                        weird_hours = (weird_files * 97) / 3600
                    else:
                        weird_hours = 0
            # return open_hours, closed_hours, extmoon_hours, intrigs_hours, weird_hours
            
            file.write(f"Total Days: {len(dfFiltered['Date'].unique())} days \n")
            file.write(f"Total Files: {len(dfFiltered['Filename'].unique())} files \n")
            file.write(f"Total Hours Data: {(len(dfFiltered['Filename'].unique()) * 97) / 3600:.1f} hours \n")
            file.write(f"Door Open Hours Data: {open_hours:.1f} hours \n")
            file.write(f"Extended Moon Hours Data: {extmoon_hours:.1f} hours \n")
            file.write(f"Door Closed Hours Data: {closed_hours:.1f} hours \n")

            unique_dates = dfFiltered['Date'].unique()
            # getOpHours(opmodes, dfFiltered)
            # returns open_hours, closed_hours, extmoon_hours, intrigs_hours, and weird_hours
            
            open_dates = []
            closed_dates = []
            extm_dates = []
            for date in unique_dates:
                day_data = dfFiltered[dfFiltered['Date'] == date]
                ## total number files and hours for each night
                day_files = len(day_data)
                day_hours_data = (night_files * 97) / 3600
                ## door open data for each night
                door_open_data = day_data[day_data['OpMode'] == 1]
                door_open_files = len(door_open_data)
                # day_open_hours = (door_open_files * 97) / 3600
                ## door closed data for each night
                door_closed_data = day_data[day_data['OpMode'] == 3]
                door_closed_files = len(door_closed_data)
                # day_closed_hours = (door_closed_files * 97) / 3600
                ## ext moon data for each night
                extmoon_data = day_data[day_data['OpMode'] == 2]
                extmoon_files = len(extmoon_data)
                # day_extmoon_hours = (extmoon_files * 97) / 3600
                ## find door position based on file types
                if extmoon_files != 0:
                    door_position = "e"
                    extm_dates.append(date)
                ## if no ext moon files but any door open files -> night = door open
                elif door_open_files != 0:
                    door_position = "o"
                    open_dates.append(date)
                ## if no ext moon or door open files (only door closed files) -> night = door closed
                else:
                    door_position = "c"
                    closed_dates.append(date)
                # check if want dates with specific door position
                door=doorIn()

                if door != "":
                    file.write("Date, Night Files, Hours \n")
                    if door == "o":
                        for d in open_dates:
                            file.write(f"{d}, {day_files}, {day_hours_data} \n")

                # if not list all dates and their data
                if door == "":
                    file.write("Date, Night Files, Hours, Door_Position \n")
                    for date in unique_dates:
                        file.write(f"{date}, {day_files}, {day_hours_data}, {door_position} \n")

                





if __name__ == "__main__":
    main()