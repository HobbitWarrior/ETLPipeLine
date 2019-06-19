import petl as etl, psycopg2 as pg, sys
from sqlalchemy import *
import csv
import urllib
import numpy as np
import datetime
import boto3


#An object that stores a single row from the CSV file, with the relevant fields stored in its
#respective properties
class Datum :
    EthUsd = 0
    Period = None
    dayOfTheWeek = None
    weekOfTheYear = None
    year = None

    def __init__(self,ethUsd = None,period = None):
        if ethUsd:
            self.EthUsd = ethUsd
        if period:
            self.Period = period

#The transform class, aggregates a list of values that represent a weekly data
#uses the NumPOy library for the calculations.
class weeklyAggregation:

    #variabels
    week = None
    year = None
    average = 0.0
    median = 0.0
    low = 0.0
    high = 0.0


    def __init__(self, list = None, week = None, year = None):
        if week:
            self.week = week
        if year:
            self.year = year
        if list:
            arr = np.array(list)
            self.average  = np.average(arr)
            self.median = np.median(arr)
            self.low = np.amin(arr)
            self.high  = np.amax(arr)


#loads the file to a CSV file
class loadToCSV:
    def writeCSV(self,list = [], header = [], dest = ''):
        with open(dest, mode = 'w',newline='') as CSVFile:
            csvLoad = csv.writer(CSVFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csvLoad.writerow(header)
            for agg in list:
                csvLoad.writerow([agg.week,agg.year,agg.average,agg.median,agg.low,agg.high])


#extract from a URL
url = "https://s3-eu-west-1.amazonaws.com/athena-dev-task/ETH_USD.csv"
webpage = urllib.request.urlopen(url)
data_reader = csv.reader(webpage.read().decode('utf-8').split('\n'),delimiter=" ")
data = []
line = 0
weeklyData = []
weekOfTheYearNumber = 0
for row in data_reader:
    if row:
        single_row = row[0].split(',')
        line += 1
        datum = Datum()
        #date
        if single_row[0]:
            #split the date
            dateList = single_row[0].split('-')
            #validate the data
            if len(dateList) == 3:
                #test the date Validity
                try:
                    ddD = int(dateList[0])
                    mmD = int(dateList[1])
                    yyyyD = int(dateList[2])
                    #if the value is invalid, just skip to the next row in the CSV file
                except ValueError:
                    dateList = []
                    datum = Datum()
                    continue
                #find the day of the week, and the week in the year -- remove
                datum.Period = datetime.date(int(dateList[0]),int(dateList[1]),int(dateList[2]))
                datum.dayOfTheWeek = datum.Period.weekday()
                datum.weekOfTheYear = datum.Period.isocalendar()[1]
                datum.year = int(dateList[0])
                # value
                if single_row[1]:
                    #check the validty of the data in ETH_USD column
                    try:
                        datum.EthUsd = float(single_row[1])
                        #print(datum.EthUsd)
                        data.append(datum)
                    except ValueError:
                        dateList = []
                        datum = Datum()
                        continue

#transfrom the data
#convert to  lists of weekely data:
weekNo = -1
year = -1
list = []
week = []

for row in data:
    #first bit of data
    if year == -1 and weekNo == -1:
        weekNo = row.weekOfTheYear
        year = row.year
        week.append(row.EthUsd)
    elif weekNo == row.weekOfTheYear and year == row.year:
        week.append(row.EthUsd)
    #new week or year
    else:
        #append to the 'list' the current weekly data, and create an empty week list
        list.append(weeklyAggregation(week,weekNo,year))
        week = []
        weekNo = row.weekOfTheYear
        year = row.year
        week.append(row.EthUsd)


#load the data to a csv file
lCSV = loadToCSV()
lCSV.writeCSV( list, ['week','year','average','median','low','high'], 'ETH_USD_agg.csv')
#End of extraction from URL
#testing bucket:
s3 = boto3.client('s3')

print(s3.upload_file(Bucket='https://s3-eu-west-1.amazonaws.com/athena-dev-task/ALEX_ZELTSER', Key='ETH_USD_agg.csv', Filename='./ETH_USD_agg.csv'))
##print(s3.Object('https://s3-eu-west-1.amazonaws.com/athena-dev-task/ALEX_ZELTSER//ETH_USD_agg.csv', 'ETH_USD_agg.csv').put(Body=open('ETH_USD_agg.csv', 'rb')))
# #print(data)
