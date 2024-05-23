import pandas as pd
import numpy as npy
import sqlite3
import os
import sys
from colorama import init as colorama_init
from colorama import Fore
from colorama import Style

colorama_init()

node = 0

def csvFetch(url):
    global node
    if node == 0:
        print(f"{Fore.GREEN}{Style.BRIGHT}Start...{Style.RESET_ALL}")
    node += 1
    if node > 0:
        print(f"{Style.DIM}{Fore.YELLOW}Fetching Data from Source {Style.RESET_ALL}{Style.BRIGHT}{Fore.YELLOW}"+str(node)+f"{Style.DIM}{Fore.YELLOW}...{Style.RESET_ALL}")
    try:
        data = pd.read_csv(url)
        dataFrame = pd.DataFrame(data)
        return dataFrame
    except:
        print(f"{Fore.RED}{Style.NORMAL}Data couldn't fetched...{Style.RESET_ALL}")
        print(f"{Fore.RED}{Style.BRIGHT}Aborted...{Style.RESET_ALL}")
        sys.exit(0)

def columnSelector(data, dropColumns, selectedColumns):
    if len(dropColumns) > 0:
        data.drop(dropColumns, inplace=True, axis=1) 
    if len(selectedColumns) > 0:
        data = data[selectedColumns]
        
    return data

def rowSelector(data, row, value):
    return data[data[row] == value]

def renameColumn(data, renameColumns):
    data.rename(columns=renameColumns, inplace = True)
    return data

def fixYear(data):
    data = data
    data["Year"] = data["Year"].str[1:]
    return data

def dropNull(data):
    print(f"{Fore.CYAN}{Style.NORMAL}Filtering rows...{Style.RESET_ALL}")
    return data.dropna(how='any',axis=0) 

def meltTable(data, keep, melt):
    print(f"{Fore.CYAN}{Style.NORMAL}Processing Data...{Style.RESET_ALL}")
    return pd.melt(data, id_vars=keep, value_vars=melt, ignore_index=True)

def dataLeftJoin(left, right, key, leftSufx, rightSufx):
    print(f"{Fore.CYAN}{Style.NORMAL}Joining Data...{Style.RESET_ALL}")
    return pd.merge(left, right, how ='left', on = key, suffixes=(leftSufx, rightSufx)) 

def csvToSQLite(data, savingPath, sqliteFileName, sqliteTableName):
    try:
        conn = sqlite3.connect(savingPath+sqliteFileName)
        data.to_sql(sqliteTableName, conn, if_exists='replace', index=False)
        print(f"{Fore.MAGENTA}{Style.BRIGHT}Data Exported... [Path: "+savingPath+"\\"+sqliteFileName+f"]{Style.RESET_ALL}")
        conn.close()  
        print(f"{Fore.GREEN}{Style.BRIGHT}End...{Style.RESET_ALL}")
    except:
        print(f"{Fore.YELLOW}{Style.DIM}Resolving Path...{Style.RESET_ALL}")
        conn = sqlite3.connect("../data/"+sqliteFileName)
        data.to_sql(sqliteTableName, conn, if_exists='replace', index=False)
        print(f"{Fore.MAGENTA}{Style.BRIGHT}Data Exported... [Check Data Folder]{Style.RESET_ALL}")
        conn.close()  
        print(f"{Fore.GREEN}{Style.BRIGHT}End...{Style.RESET_ALL}")

targetedPath = os.path.join(os.getcwd(), "data\\")  

url1 = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
dropColumns1 = ["ObjectId", "Indicator", "Source", "CTS_Code", "CTS_Name", "CTS_Full_Descriptor",  "F2000","F2001","F2002","F2003","F2004","F2005","F2006","F2007","F2008","F2009"]
selectedColumns1 = ["Country","ISO3","Unit", "F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]
renameColumns1 = {"variable":"Year", "value":"Temperature"}
data1 = fixYear(renameColumn(meltTable(columnSelector(csvFetch(url1), dropColumns1, selectedColumns1), ['ISO3', "Country"], ["F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]), renameColumns1))

url2 = "https://opendata.arcgis.com/datasets/b13b69ee0dde43a99c811f592af4e821_0.csv"
dropColumns2 = ["ObjectId", "Source", "CTS_Code", "CTS_Name", "CTS_Full_Descriptor",  "F2000","F2001","F2002","F2003","F2004","F2005","F2006","F2007","F2008","F2009",]
selectedColumns2 = ["ISO3","Unit", "F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]
renameColumns2 = {"variable":"Year", "value":"Incident"}
data2 = fixYear(renameColumn(meltTable(columnSelector(rowSelector(csvFetch(url2), "Indicator", "Climate related disasters frequency, Number of Disasters: TOTAL"), dropColumns2, selectedColumns2), ['ISO3'], ["F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]), renameColumns2))

csvToSQLite(dropNull(dataLeftJoin(data1, data2, ["ISO3", "Year"], "_temp", "_incident")), targetedPath, "SurfaceTemperatureChangeOnClimate_relatedDisaster.sqlite", "Temp_Disaster")
