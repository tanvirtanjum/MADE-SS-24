import pandas as pd
import sqlite3
import os
import sys
from utils.alerts import *

node = 0

class Pipeline():
    PipelineData : object
    url : str
    dropColumns : object
    selectedColumns : object
    
    def __init__(self, PipelineData = None):
        self.PipelineData = PipelineData
        
        
    def csvFetch(self):
        global node
        
        if node == 0:
            success("Start...")
        node += 1
        if node > 0:
            progress("Fetching Data from Source", node)
        try:
            data = pd.read_csv(self.url)
            self.PipelineData = pd.DataFrame(data)
        except:
            error("Data couldn't fetched...")
            fail("Aborted...")
            sys.exit(0)
            

    def columnSelector(self):
        if len(self.dropColumns) > 0:
            self.PipelineData.drop(self.dropColumns, inplace = True, axis = 1) 
        if len(self.selectedColumns) > 0:
            self.PipelineData = self.PipelineData[self.selectedColumns]
            

    def rowSelector(self, row : str, value : str):
        self.PipelineData = self.PipelineData[self.PipelineData[row] == value]
        

    def renameColumn(self, renameColumns : object):
        self.PipelineData.rename(columns = renameColumns, inplace = True)
        

    def fixYear(self):
        self.PipelineData["Year"] = self.PipelineData["Year"].str[1:]
        

    def dropNull(self):
        primary("Filtering rows...")
        self.PipelineData = self.PipelineData.dropna(how = "any", axis = 0) 
        

    def meltTable(self, keep : object, melt : object):
        primary("Processing Data...")
        self.PipelineData =  pd.melt(self.PipelineData, id_vars=keep, value_vars=melt, ignore_index=True)
        

    def dataLeftJoin(self, left : object, right : object, key : object, leftSufx : str, rightSufx : str):
        primary("Joining Data...")
        self.PipelineData = pd.merge(left, right, how ='left', on = key, suffixes=(leftSufx, rightSufx))
        
        
    def csvToSQLite(self, savingPath : str, sqliteFileName : str, sqliteTableName : str):
        try:
            conn = sqlite3.connect(savingPath+sqliteFileName)
            self.PipelineData.to_sql(sqliteTableName, conn, if_exists='replace', index=False)
            complete("Data Exported... [Path: "+savingPath+"\\"+sqliteFileName+"]")
            conn.close()  
            success("End...")
            
        except:
            error("Path Error...")
            progress2("Resolving Path...")
            savingPath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)+"\n")) + "\\data\\"
            conn = sqlite3.connect("../data/"+sqliteFileName)
            self.PipelineData.to_sql(sqliteTableName, conn, if_exists='replace', index=False)
            successDim("Path Resolved...")
            complete("Data Exported... [Path: "+savingPath+"\\"+sqliteFileName+"]")
            conn.close()  
            success("End...")


def main():
    # 1st Data Source
    p1 = Pipeline()
    p1.url = "https://opendata.arcgis.com/datasets/4063314923d74187be9596f10d034914_0.csv"
    p1.dropColumns = ["ObjectId", "Indicator", "Source", "CTS_Code", "CTS_Name", "CTS_Full_Descriptor",  "F2000","F2001","F2002","F2003","F2004","F2005","F2006","F2007","F2008","F2009"]
    p1.selectedColumns = ["Country","ISO3","Unit", "F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]
    renameColumns1 = {"variable":"Year", "value":"Temperature"}
    p1.csvFetch()
    p1.columnSelector()
    p1.meltTable(['ISO3', "Country"], ["F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"])
    p1.renameColumn(renameColumns1)
    p1.fixYear()

    # 2nd Data Source
    p2 = Pipeline()
    p2.url = "https://opendata.arcgis.com/datasets/b13b69ee0dde43a99c811f592af4e821_0.csv"
    p2.dropColumns = ["ObjectId", "Source", "CTS_Code", "CTS_Name", "CTS_Full_Descriptor",  "F2000","F2001","F2002","F2003","F2004","F2005","F2006","F2007","F2008","F2009",]
    p2.selectedColumns = ["ISO3","Unit", "F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"]
    renameColumns2 = {"variable":"Year", "value":"Incident"}
    p2.csvFetch()
    p2.rowSelector("Indicator", "Climate related disasters frequency, Number of Disasters: TOTAL")
    p2.columnSelector()
    p2.meltTable(['ISO3'], ["F2010","F2011","F2012","F2013","F2014","F2015","F2016","F2017","F2018","F2019","F2020"])
    p2.renameColumn(renameColumns2)
    p2.fixYear()

    # Final Data
    p3 = Pipeline()
    p3.dataLeftJoin(p1.PipelineData, p2.PipelineData, ["ISO3", "Year"], "_temp", "_incident")
    p3.dropNull()
    # Saving Data
    targetedPath = os.path.join(os.getcwd(), "data\\")  
    fileName = "SurfaceTemperatureChangeOnClimate_relatedDisaster.sqlite"
    dbName = "Temp_Disaster"
    p3.csvToSQLite(targetedPath, fileName, dbName)
    
    
if __name__ == "__main__":
    main()
