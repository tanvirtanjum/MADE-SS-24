<style>
    body {text-align: justify}
</style>

### Effects of Surface Temperature Change on Climate-related Disasters

> ##### 1. Introduction
The global climate change is a significant problem requiring urgent attention as it affects both human and natural systems. One of the most important issues concerning climate change is the increase in surface temperatures, leading to increased frequency and severity of climate related disasters. This data engineering project seeks to investigate how these changes in surface temperature are responsible for various weather occurrences. By examining historical records and applying sophisticated data analysis methods, this project attempts to explain these patterns.

> ##### 2. Used Data

Leveraging two open datasets from well-established repository [(IMF)](https://www.imf.org), this project was performed. They provided a lot of information that was crucial to my research. The datasets are free-to-use for study purpose [(Terms)](https://www.imf.org/external/terms.htm). For this project I used two datasets.
- [Annual Surface Temperature Change](https://climatedata.imf.org/datasets/4063314923d74187be9596f10d034914/explore)
- [Climate-related Disasters Frequency](https://climatedata.imf.org/datasets/b13b69ee0dde43a99c811f592af4e821/explore)

The extensive data points found in these datasets enabled me to make detailed analyses. To take full advantage of the datasets in this instance, I created and executed a data pipeline [(Source Code)](https://github.com/tanvirtanjum/MADE-SS-24/blob/main/project/pipeline.py) to help structure a workflow for the data back end.
<figure align="center" style="width:100%">
    <img src="./ETL.png"
         alt="ETL Pipeline Flow"
         style="width:60%">
    <figcaption>Figure 1: ETL Pipeline</figcaption>
</figure>
Creating this pipeline was an important part of the project. Firstly, it was used to fetch the data from the source.
<figure align="center" style="width:100%">
    <img src="./Annual Surface Temperature Change.png"
         alt="Annual Surface Temperature Change"
         style="width:80%">
    <figcaption>Figure 2: DB - Source 1</figcaption>
    <img src="./Climate-related Disasters Frequency.png"
         alt="Climate-related Disasters Frequency"
          style="width:80%">
    <figcaption>Figure 3: DB - Source 2</figcaption>
</figure>
Then it was used to sort out the work conducted by merging and creating a singular format for data transference and analysis. Some of the steps included in the process were data cleaning, transformation, and validation to ensure that all of the steps were in order and that the data flow was reliable. 
<figure align="center" style="width:100%">
    <img src="./FinalDB.png"
         alt="Final Merged Data"
         style="width:80%">
    <figcaption>Figure 4: Final Merged Data</figcaption>
</figure>
By doing this, I was able to greatly enhance the quality of the research while showing how important data engineering is to any research project.

> ##### 3. Analysis

> ##### 4. Conclusions
