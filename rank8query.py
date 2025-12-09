##### from userUpdateDatabase.py
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

#### from getRank8.py
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

def startDate():
  startDate=input(color.RED + "start date:" + color.END)
  print(startDate)
  return startDate

def endDate():
  endDate=input(color.RED + "end date:" + color.END)
  print(endDate)
  return endDate

def inputhvValues():
    # user input
    hvVal=input(color.RED + "hvValues1:" + color.END)
    print(hvVal)
    return hvVal

def inputCurrents():
    # user input
    current=input(color.RED + "hvcurrents1:" + color.END)
    print(current)
    return current

def inputSunAlt():
    # user input
    sunAlt=input(color.RED + "sun altitude:" + color.END)
    print(sunAlt)
    return sunAlt

def inputMoonAlt():
    # user input
    moonAlt=input(color.RED + "moon altitude:" + color.END)
    print(moonAlt)
    return moonAlt

def inputOpmode():
    # user input
    opMode=input(color.RED + "operation mode:" + color.END)
    print(opMode)
    return opMode

def inputDoor():
    # user input
    door=input(color.RED + "door position:" + color.END)
    print(door)
    return door

def getopHours(o):
  # o = opmode = dfFiltered['OpMode'] ?
  open_hours = 0
  extmoon_hours = 0
  closed_hours = 0
  for i in o:
    if i == 1:
        total_open_data = dfFiltered[o == i]
        if len(total_open_data) != 0:          
            open_files = len(total_open_data)
            open_hours = (open_files * 97) / 3600
        else:
            open_hours = 0
    elif i == 2:
        total_extmoon_data = dfFiltered[o == i]
        if len(total_extmoon_data) != 0:          
            total_extmoon_files = len(total_extmoon_data)
            extmoon_hours = (total_extmoon_files * 97) / 3600
        else:
            extmoon_hours = 0
    elif i ==3:
        total_closed_data = dfFiltered[o == i]
        if len(total_closed_data) != 0:          
            closed_files = len(total_closed_data)
            closed_hours = (closed_files * 97) / 3600
        else:
            closed_hours = 0
  return open_hours, extmoon_hours, closed_hours    

def nightData(dates):
  #dates = unique_dates = dfFiltered['Date'].unique()
  # all data for night
  for date in dates:
    day_data = dfFiltered[dfFiltered['Date'] == date]
    night_files = len(day_data)
    night_hours_data = (night_files * 97) / 3600
    # nightly door open data
    door_open_data = day_data[day_data['OpMode'] == 1]
    door_open_files = len(door_open_data)
    # nightly door closed data
    door_closed_data = day_data[day_data['OpMode'] == 3]
    door_closed_files: int = len(door_closed_data)
    # nightly ext moon data
    extmoon_data = day_data[day_data['OpMode'] == 2]
    extmoon_files = len(extmoon_data)
    # nightly door position
    if extmoon_files != 0:
      door_position = "e"
    elif door_open_files != 0:
      door_position = "o"
    else:
      door_position = "c"
  return night_files, night_hours_data, door_position

# needs args
def totalData():
  unique_dates = dfFiltered['Date'].unique()
  total_days = len(unique_dates)
  unique_files = dfFiltered['Filename'].unique()
  total_files = len(unique_files)
  hours_data = (total_files * 97) / 3600
  return unique_dates, total_days, unique_files, total_files, hours_data
# needs args
def writeFileHeader():
  with open('rank8Files.txt', mode='w', newline='') as file:
    file.write("Rank 8 queried data \n")
    file.write(f"Period of time : {dfFiltered['Date'].min()} to {dfFiltered['Date'].max()} \n")
    file.write(f"Total Days: {total_days} days \n")
    file.write(f"Total Files: {total_files} files \n")
    file.write(f"Total Hours Data: {hours_data} hours \n")
    

def QDF(df):
  

def main(): 
  print("please enter query info")
  
  # Local host lines for access
  host = 'localhost'
  port = 8086
  username = 'admin'
  database = 'TDFiles'
  password = 'Ttys@210'
  
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
  
  result = client.query(f'SELECT "hvValues1", "hvcurrents1", "Filename" FROM "DataInfo"')
  
  
  hvValues1 = []
  hvcurrents1 = []
  Filename = []
  
  for point in result.get_points():
      hvValues1.append(point['hvValues1'])
      hvcurrents1.append(point['hvcurrents1'])
      Filename.append(point['Filename'])
  
  data = np.array([hvValues1, hvcurrents1, Filename]).T
  # Create a DataFrame from the array
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
  # Create a DataFrame from the array
  dfCelestial = pd.DataFrame(data, columns=['sunAltitude', 'moonAltitude', 'Filename'])
  
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
  
  # reduce the data by removing the rows where the ranking is not 8
  dfFiltered = dfMerged[(dfMerged['Ranking'] == 8)]
  
  # create a new column with the data from the filename column so that it is yyyy-mm-dd
  dfFiltered['Date'] = (pd.to_datetime(dfFiltered['Filename'].str[12:22])).astype(str)
  dfFiltered['Date'] = (dfFiltered['Date'].str.replace('-','')).astype(int)
  
  startDate = startDate()
  endDate  = endDate()
  
  if startDate == "":
    dfFiltered = dfFiltered[dfFiltered['Date'] >= 20250101]
  else:
    dfFiltered = dfFiltered[dfFiltered['Date'] >= startDate]
  if endDate == "":
    dfFiltered = dfFiltered[dfFiltered['Date'] <= 20251231]
  else:
    dfFiltered = dfFiltered[dfFiltered['Date'] <= endDate]

  hvVal = inputhvValues()
  
  if hvVal != "":
    dfFiltered = dfFiltered[(dfFiltered['hvValues1'] == hvVal)]

  current = inputCurrents()

  if current != "":
    dfFiltered = dfFiltered[(dfFiltered['hvcurrents1'] == current)]

  sunAlt = inputSunAlt()
  
  if sunAlt != "":
    dfFiltered = dfFiltered[(dfFiltered['sunAltitude'] == sunAlt)]

  moonAlt = inputMoonAlt()
  
  if moonAlt != "":
    dfFiltered = dfFiltered[(dfFiltered['moonAltitude'] == moonAlt)]

  
  unique_dates = dfFiltered['Date'].unique()

  opMode = inputOpmode()
  opmode = dfFiltered['OpMode']
  if opMode != "":
    dfFiltered = dfFiltered[(opmode == opMode)]
  else: 
    getopHours(opmode)
    nightData(unique_dates)

  door = inputDoor()
  if door != "":
     



if __name__ == "__main__":
    main()
  
