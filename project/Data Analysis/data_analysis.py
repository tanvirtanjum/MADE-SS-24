import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if module_path not in sys.path:
    sys.path.append(module_path)
import pipeline
    
class Analysis:
    pipelineInfo = None
    data = None
    data_agg = None
    pivot_temperature = None
    pivot_incident = None
    p = pipeline.Pipeline()
    output_dir = None
    
    def __init__(self, pipelineInfo = None, data = None, data_agg = None, pivot_temperature = None, pivot_incident = None, p = pipeline.Pipeline(), output_dir = None):
        self.pipelineInfo = pipeline.main()
        self.getData()
        self.output_dir = output_dir
        self.createOutputDirectory()
     
        
    def getData(self):
        conn = sqlite3.connect(self.pipelineInfo["Path"])
        cursor = conn.cursor()

        cursor.execute(f'''SELECT   ISO3, 
                                    Country, 
                                    Year, 
                                    Temperature, 
                                    Incident 
                            FROM {self.pipelineInfo["Table"]};
        ''')
        
        self.data = pd.DataFrame(cursor.fetchall(), columns=['ISO3', 'Country', 'Year', 'Temperature', 'Incident'])
        conn.close()
        os.remove(self.pipelineInfo["Path"])

        
    def limitDataByISO3Year(self, ISO, StartYear, EndYear):
        self.p.PipelineData = self.data
        self.p.limitISO3(ISO)
        self.p.limitYear(StartYear, EndYear)
        self.data = self.p.PipelineData
    
    
    def createOutputDirectory(self):
        print("Creating Directory...")
        if(self.output_dir is None):
            self.output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'output'))
        if(not os.path.exists(self.output_dir)):
            os.makedirs(self.output_dir)
        if(os.path.exists(self.output_dir)):
            print("Created...")
     
            
    def save(self, fileName):
        output_path = os.path.join(self.output_dir, fileName)
        plt.savefig(output_path)
        print(f"Plot saved to {output_path}")
        plt.show()
     
              
    def aggregate(self):
        self.data_agg = self.data.groupby(['Year', 'Country'], as_index=False).agg({'Incident': 'mean', 'Temperature': 'mean'})   
      
          
    def pivot(self, data):
        self.pivot_incident = data.pivot(index='Year', columns='Country', values='Incident')
        self.pivot_temperature = data.pivot(index='Year', columns='Country', values='Temperature')
      
        
    def plotIncident(self):
        fig, ax1 = plt.subplots(figsize=(8, 6))
        self.pivot_incident.plot(ax=ax1, marker='o', linestyle='solid', linewidth=2)
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Incident')
        ax1.set_title('Incident by Country and Year')
        ax1.grid(True, which='both', linestyle='solid', linewidth=0.5)
        self.save("climate_data_incident.png")
    
    
    def plotTemperature(self):
        fig, ax2 = plt.subplots(figsize=(8, 6))
        cmap = plt.get_cmap('tab10')
        for i, country in enumerate(self.pivot_temperature.columns):
            self.pivot_temperature[country].plot(ax=ax2, marker='o', linestyle='dashdot', linewidth=1.5, color=cmap(i), label=country)
            ax2.set_xlabel('Year')
            ax2.set_ylabel('Surface Temperature (°C)')
            ax2.set_title('Surface Temperature by Country and Year')
            ax2.grid(True, which='both', linestyle='dashdot', linewidth=0.5)
            ax2.legend()
            
        self.save("climate_data_temperature.png")
      
        
    def plotTemperatureXIncident(self):
        fig, ax1 = plt.subplots(figsize=(14, 10))
        self.pivot_incident.plot(ax=ax1, marker='o', legend=False)
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Incident', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax2 = ax1.twinx()
        self.pivot_temperature.plot(ax=ax2, marker='s', linestyle='--', linewidth=1, legend=False)
        ax2.set_ylabel('Surface Temperature (°C)', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', bbox_to_anchor=(0, 1), title='Incident and Temperature')
        plt.title('Incident and Surface Temperature by Country and Year')
        self.save("climate_data_plot_lines.png")
        

def main():
    report = Analysis()
    report.limitDataByISO3Year(["IDN", "MOZ", "ITA", "USA", "CHL", "AUS"], 2010, 2019)
    report.aggregate()
    report.pivot(report.data_agg)
    report.plotIncident()
    report.plotTemperature()
    report.pivot(report.data)
    report.plotTemperatureXIncident()
    
if __name__ == "__main__":
    main()