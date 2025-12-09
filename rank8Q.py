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

# user input defs

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



##

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
    #print(data)

    # Create a DataFrame from the array
    dfRanking = pd.DataFrame(data, columns=['Filename', 'Ranking', 'OpMode'])
    # print(dfRanking)


    result = client.query(f'SELECT "hvValues1", "hvcurrents1", "Filename" FROM "DataInfo"')


    hvValues1 = []
    hvcurrents1 = []
    Filename = []

    for point in result.get_points():
        hvValues1.append(point['hvValues1'])
        hvcurrents1.append(point['hvcurrents1'])
        Filename.append(point['Filename'])

    data = np.array([hvValues1, hvcurrents1, Filename]).T
    # print(data)
    # Create a DataFrame from the array
    dfData = pd.DataFrame(data, columns=['hvValues1', 'hvcurrents1', 'Filename'])
    # print(dfData)

    result = client.query(f'SELECT "sunAltitude", "moonAltitude", "Filename" FROM "CelestialInfo"')

    sunAltitude = []
    moonAltitude = []
    Filename = []
    for point in result.get_points():
        sunAltitude.append(point['sunAltitude'])
        moonAltitude.append(point['moonAltitude'])
        Filename.append(point['Filename'])

    data = np.array([sunAltitude, moonAltitude, Filename]).T
    #print(data)
    # Create a DataFrame from the array
    dfCelestial = pd.DataFrame(data, columns=['sunAltitude', 'moonAltitude', 'Filename'])
    # print(dfCelestial)

    # Merge the DataFrames on 'Filename'
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

    dfFiltered = dfMerged[(dfMerged['Ranking'] == 8)]
    # print(dfFiltered)

    # create a new column with the data from the filename column so that it is yyyy-mm-dd
    dfFiltered['Date'] = (pd.to_datetime(dfFiltered['Filename'].str[12:22])).astype(str)
    dfFiltered['Date'] = (dfFiltered['Date'].str.replace('-','')).astype(int)
    # print(dfFiltered)

    ## set start/end dates = start/end dates input defs
    startDate=startDateIn()
    endDate=endDateIn()
    ## check for start/end date user input
    if startDate != "":
        dfFiltered = dfFiltered[dfFiltered['Date'] >= startDate]
    else:
        dfFiltered = dfFiltered[dfFiltered['Date'] >= 20250101]
    if endDate != "":
        dfFiltered = dfFiltered[dfFiltered['Date'] >= endDate]
    else:
        dfFiltered = dfFiltered[dfFiltered['Date'] >= 20251231]
    # print(dfFiltered)
    ##
    dfFiltered['OpMode'] = dfMerged['OpMode']
    opmode = dfFiltered['OpMode']

    # """ 
    # create 1 csv file  start with 
    # total days = unique values in the Date column, 
    # total files = unique values in the Filename column, 
    # hours data = total  files *  97 seconds per file / 3600 seconds per hour

    # then the 2nd part will be the data for each day,
    # so  
    # night, files, hours data
    # """

    datatype=dORf()
    ## if looking for indiv files: look for these inputs and reduce df to them
    if datatype == "files":
        hvVal=hvValIn()
        if hvVal != 0:
            dfFiltered = dfFiltered[dfFiltered['hvValues1'] == hvVal]
        hvCurr=hvCurrIn()
        if hvCurr != 0:
            dfFiltered = dfFiltered[dfFiltered['hvCurrents1'] == hvCurr]
        sunAlt=sunAltIn()
        if sunAlt != 0:
            dfFiltered = dfFiltered[dfFiltered['sunAltitude'] == sunAlt]
        moonAlt=moonAltIn()
        if moonAlt != 0:
            dfFiltered = dfFiltered[dfFiltered['moonAltitude'] == moonAlt]
        op=opModeIn()
        if op != 0:
            dfFiltered = dfFiltered[opmode == op]
    elif datatype == "dates":
        open_hours = 0
        extmoon_hours = 0
        closed_hours = 0
        ## just to see other opmode files contributions
        weird_hours = 0
        intrigs_hours = 0

        for i in opmode:
            if i == 1:
                total_open_data = dfFiltered[dfFiltered['OpMode'] == i]
                if len(total_open_data) != 0:          
                    open_files = len(total_open_data)
                    open_hours = (open_files * 97) / 3600
                else:
                    open_hours = 0
            elif i == 2:
                total_extmoon_data = dfFiltered[dfFiltered['OpMode'] == i]
                if len(total_extmoon_data) != 0:          
                    total_extmoon_files = len(total_extmoon_data)
                    extmoon_hours = (total_extmoon_files * 97) / 3600
                else:
                    extmoon_hours = 0
            elif i ==3:
                total_closed_data = dfFiltered[dfFiltered['OpMode'] == i]
                if len(total_closed_data) != 0:          
                    closed_files = len(total_closed_data)
                    closed_hours = (closed_files * 97) / 3600
                else:
                    closed_hours = 0
            ## just to see other opmode files contnributions
            elif i == 6:
                total_weird_data = dfFiltered[dfFiltered['OpMode'] == i]
                if len(total_weird_data) != 0:          
                    weird_files = len(total_weird_data)
                    weird_hours = (weird_files * 97) / 3600
                else:
                    weird_hours = 0
            elif i == 0:
                total_intrigs_data = dfFiltered[dfFiltered['OpMode'] == i]
                if len(total_intrigs_data) != 0:          
                    intrigs_files = len(total_intrigs_data)
                    intrigs_hours = (intrigs_files * 97) / 3600
                else:
                    intrigs_hours = 0


        # create a txt file
        with open('rank8Files.txt', mode='w', newline='') as file:
            # write the header
            file.write("Trinity Demonstrator Database Output - Rank 8 Files \n")
            file.write("")
            file.write(f"Period of time : {dfFiltered['Date'].min()} to {dfFiltered['Date'].max()} \n")
            file.write(f"Total Days: {len(dfFiltered['Date'].unique())} days \n")
            file.write(f"Total Files: {len(dfFiltered['Filename'].unique())} files \n")
            file.write(f"Total Hours Data: {(len(dfFiltered['Filename'].unique()) * 97) / 3600:.1f} hours \n")
            file.write(f"Door Open Hours Data: {open_hours:.1f} hours \n")
            file.write(f"Extended Moon Hours Data: {extmoon_hours:.1f} hours \n")
            file.write(f"Door Closed Hours Data: {closed_hours:.1f} hours \n")
            file.write("\n")
            file.write("Date, Night Files, Hours Data, Door Position \n")
        ## old:
            # file.write("Date, Night Files, Hours Data \n")

            

            # write the data for each day
            # get the unique values in the Date column
            unique_dates = dfFiltered['Date'].unique()
            total_days = len(unique_dates)
            
            
            # get the unique values in the Filename column
            unique_files = dfFiltered['Filename'].unique()
            total_files = len(unique_files)
            # print(total_files)

            # calculate the hours data
            hours_data = (total_files * 97) / 3600
        ## PER NIGHT
            for date in unique_dates:
                # print(date)
                ## df with all files for each night
                day_data = dfFiltered[dfFiltered['Date'] == date]
                # print(day_data)
                ## number of files for each night
                night_files = len(day_data)
                # print(night_files)
                ## number of hours for each night
                night_hours_data = (night_files * 97) / 3600
                # print(night_hours_data)

            ## old
                # file.write(f"{date}, {night_files}, {night_hours_data:.2f}\n")

            
            ## door position identification:
                ## find files with OpMode 1 - door open
                door_open_data = day_data[day_data['OpMode'] == 1]
                door_open_files = len(door_open_data)

                # print(date)
                # print(door_open_data)
                # print(door_open_files)

                ## find files with OpMode 2 - door closed
                door_closed_data = day_data[day_data['OpMode'] == 3]
                door_closed_files = len(door_closed_data)

                # print(date)
                # print(door_closed_data)
                # print(door_closed_files)

                ## find files with OpMode 2 - ext moon
                extmoon_data = day_data[day_data['OpMode'] == 2]
                extmoon_files = len(extmoon_data)

                # print(date)
                # print(extmoon_data)
                # print(extmoon_files)

            ## binary 
                num = ["0", "0", "0"]
            
                if door_open_files != 0:
                    num[0] = "1"
                if extmoon_files != 0:
                    num[1] = "1"
                if door_closed_files != 0:
                    num[2] = "1"
                binary_result = "".join(num)
                # print(f"result: {date}, {binary_result}")

            ## based on if there are any files with a) ext moon data or b) door open data
                ## if any extmoon files -> night = extmoon
                if extmoon_files != 0:
                    door_position = "e"
                    return door_position
                ## if no ext moon files but any door open files -> night = door open
                elif door_open_files != 0:
                    door_position = "o"
                    return door_position
                ## if no ext moon or door open files (only door closed files) -> night = door closed
                else:
                    door_position = "c"
                    return door_position

        # # include door position indicator per night
        #     file.write(f"{date}, {night_files}, {night_hours_data:.2f}, {door_position} \n")

            door=doorIn()
            if door == "o":
                file.write(f"{date}, {night_files}, {night_hours_data:.2f} \n")
            elif door == "e":
                file.write(f"{date}, {night_files}, {night_hours_data:.2f} \n")
            elif door == "c":
                file.write(f"{date}, {night_files}, {night_hours_data:.2f} \n")
            


    print("Rank 8 Files Data written to rank8Files.txt")
        

        # writer.writerow(['Date', 'Total Files', 'Total Days', 'Hours Data'])
        
        
        # # write the data
        # writer.writerow([unique_dates[0], total_files, total_days, hours_data])

if __name__ == "__main__":
    main()
        

