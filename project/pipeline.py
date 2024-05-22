import pandas as pd
import numpy as npy
import sqlite3

node = 0

def incidentsSelector(data, indicator):
    return data[data.Indicator == indicator]

def csvFetch(url):
    global node
    if node == 0:
        print("Start...")
    node += 1
    if node > 0:
        print("Fetching Data from Source "+str(node)+"...")
    data = pd.read_csv(url)
    dataFrame = pd.DataFrame(data)
    return dataFrame

def columnSelector(data, dropColumns, selectedColumns):
    if len(dropColumns) > 0:
        data.drop(dropColumns, inplace=True, axis=1) 
    if len(selectedColumns) > 0:
        data = data[selectedColumns]
        
    return data

def renameColumn(data, renameColumns):
    data.rename(columns=renameColumns, inplace = True)
    return data

def dataLeftJoin(left, right, key, leftSufx, rightSufx):
    print("Processing Data...")
    return pd.merge(left, right, how ='left', on =key, suffixes=(leftSufx, rightSufx)) 

def dropNull(data):
    return data.dropna(how='any',axis=0) 

def csvToSQLite(data, savingPath, sqliteFileName, sqliteTableName):
    conn = sqlite3.connect(savingPath+sqliteFileName)
    data.to_sql(sqliteTableName, conn, if_exists='replace', index=False)
    print("Data Exported... [$root"+savingPath[2 : ]+sqliteFileName+"]")
    conn.close()
    
    print("End...")

targetedPath = "../data/"   

url1 = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
dropColumns1 = ["ObjectId", "Indicator", "Source", "CTS_Code", "CTS_Name", "CTS_Full_Descriptor",  "F2000","F2001","F2002","F2003","F2004","F2005","F2006","F2007","F2008","F2009"]
selectedColumns1 = ["Country","ISO3","Unit", "F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]
# renameColumns1 = {"F2010":"F2010_Temp", "F2011":"F2011_Temp","F2012":"F2012_Temp","F2013":"F2013_Temp","F2014":"F2014_Temp","F2015":"F2015_Temp","F2016":"F2016_Temp","F2017":"F2017_Temp","F2018":"F2018_Temp","F2019":"F2019_Temp","F2020":"F2020_Temp"}
data1 = columnSelector(csvFetch(url1), dropColumns1, selectedColumns1)

url2 = "https://opendata.arcgis.com/datasets/b13b69ee0dde43a99c811f592af4e821_0.csv"
dropColumns2 = ["ObjectId", "Source", "CTS_Code", "CTS_Name", "CTS_Full_Descriptor",  "F2000","F2001","F2002","F2003","F2004","F2005","F2006","F2007","F2008","F2009",]
selectedColumns2 = ["ISO3","Unit", "F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]
data2 = columnSelector(incidentsSelector(csvFetch(url2), "Climate related disasters frequency, Number of Disasters: TOTAL"), dropColumns2, selectedColumns2)

csvToSQLite(dropNull(dataLeftJoin(data1, data2, "ISO3", "_temp", "_incident")), targetedPath, "SurfaceTemperatureChangeOnClimate_relatedDisaster.sqlite", "Temp_Disaster")