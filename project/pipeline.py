import pandas as pd
import sqlite3
import os
import sys
from utils.alerts import *

node = 0

class Pipeline():
    PipelineData : object
    
    def __init__(self, PipelineData = None):
        self.PipelineData = PipelineData
        
    def csvFetch(self, url : str):
        global node
        
        if node == 0:
            success("Start...")
        node += 1
        if node > 0:
            progress("Fetching Data from Source", node)
        try:
            data = pd.read_csv(url)
            dataFrame = pd.DataFrame(data)
            return dataFrame
        except:
            error("Data couldn't fetched...")
            fail("Aborted...")
            sys.exit(0)

    def columnSelector(self, data : object, dropColumns : object, selectedColumns : object):
        if len(dropColumns) > 0:
            data.drop(dropColumns, inplace = True, axis = 1) 
        if len(selectedColumns) > 0:
            data = data[selectedColumns]
            
        return data

    def rowSelector(self, data : object, row : str, value : str):
        return data[data[row] == value]

    def renameColumn(self, data : object, renameColumns : object):
        data.rename(columns = renameColumns, inplace = True)
        return data

    def fixYear(self, data : object):
        data = data
        data["Year"] = data["Year"].str[1:]
        return data

    def dropNull(self, data : object):
        primary("Filtering rows...")
        return data.dropna(how = "any", axis = 0) 

    def meltTable(self, data : object, keep : object, melt : object):
        primary("Processing Data...")
        return pd.melt(data, id_vars=keep, value_vars=melt, ignore_index=True)

    def dataLeftJoin(self, left : object, right : object, key : object, leftSufx : str, rightSufx : str):
        primary("Joining Data...")
        return pd.merge(left, right, how ='left', on = key, suffixes=(leftSufx, rightSufx)) 

    def csvToSQLite(self, data : object, savingPath : str, sqliteFileName : str, sqliteTableName : str):
        try:
            conn = sqlite3.connect(savingPath+sqliteFileName)
            data.to_sql(sqliteTableName, conn, if_exists='replace', index=False)
            complete("Data Exported... [Path: "+savingPath+"\\"+sqliteFileName+"]")
            conn.close()  
            success("End...")
            
        except:
            error("Path Error...")
            progress2("Resolving Path...")
            savingPath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)+"\n")) + "\\data\\"
            conn = sqlite3.connect("../data/"+sqliteFileName)
            data.to_sql(sqliteTableName, conn, if_exists='replace', index=False)
            successDim("Path Resolved...")
            complete("Data Exported... [Path: "+savingPath+"\\"+sqliteFileName+"]")
            conn.close()  
            success("End...")


def main():
    # 1st Data Source
    p1 = Pipeline()
    url1 = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
    dropColumns1 = ["ObjectId", "Indicator", "Source", "CTS_Code", "CTS_Name", "CTS_Full_Descriptor",  "F2000","F2001","F2002","F2003","F2004","F2005","F2006","F2007","F2008","F2009"]
    selectedColumns1 = ["Country","ISO3","Unit", "F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]
    renameColumns1 = {"variable":"Year", "value":"Temperature"}
    p1.PipelineData = p1.csvFetch(url1)
    p1.PipelineData = p1.columnSelector(p1.PipelineData, dropColumns1, selectedColumns1)
    p1.PipelineData = p1.meltTable(p1.PipelineData, ['ISO3', "Country"], ["F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"])
    p1.PipelineData = p1.renameColumn(p1.PipelineData, renameColumns1)
    p1.PipelineData = p1.fixYear(p1.PipelineData)

    # 2nd Data Source
    p2 = Pipeline()
    url2 = "https://opendata.arcgis.com/datasets/b13b69ee0dde43a99c811f592af4e821_0.csv"
    dropColumns2 = ["ObjectId", "Source", "CTS_Code", "CTS_Name", "CTS_Full_Descriptor",  "F2000","F2001","F2002","F2003","F2004","F2005","F2006","F2007","F2008","F2009",]
    selectedColumns2 = ["ISO3","Unit", "F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]
    renameColumns2 = {"variable":"Year", "value":"Incident"}
    p2.PipelineData = p2.csvFetch(url2)
    p2.PipelineData = p2.rowSelector(p2.PipelineData, "Indicator", "Climate related disasters frequency, Number of Disasters: TOTAL")
    p2.PipelineData = p2.columnSelector(p2.PipelineData, dropColumns2, selectedColumns2)
    p2.PipelineData = p2.meltTable(p2.PipelineData, ['ISO3'], ["F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"])
    p2.PipelineData = p2.renameColumn(p2.PipelineData, renameColumns2)
    p2.PipelineData = p2.fixYear(p2.PipelineData)

    # Final Data
    p3 = Pipeline()
    p3.PipelineData = p3.dataLeftJoin(p1.PipelineData, p2.PipelineData, ["ISO3", "Year"], "_temp", "_incident")
    p3.PipelineData = p3.dropNull(p3.PipelineData)

    # Saving Data
    targetedPath = os.path.join(os.getcwd(), "data\\")  
    fileName = "SurfaceTemperatureChangeOnClimate_relatedDisaster.sqlite"
    dbName = "Temp_Disaster"
    p3.csvToSQLite(p3.PipelineData, targetedPath, fileName, dbName)
    
if __name__ == "__main__":
    main()
