### Effects of Surface Temperature Change on Climate-related Disasters


> ##### 1. Introduction
<div style="text-align: justify;">
The global climate change is a significant problem requiring urgent attention as it affects both human and natural systems. One of the most important issues concerning climate change is the increase in surface temperatures, leading to increased frequency and severity of climate related disasters. This data engineering project seeks to investigate how these changes in surface temperature are responsible for various weather occurrences. By examining historical records and applying sophisticated data analysis methods, this project attempts to explain these patterns.
</div>
</br>

> ##### 2. Used Data
<div style="text-align: justify;">
Leveraging two open datasets from well-established repository <a href="https://www.imf.org">(IMF)</a>, this project was performed. They provided a lot of information that was crucial to my research. The datasets are free-to-use for study purpose <a href="https://www.imf.org/external/terms.htm">(Terms)</a>. For this project I used two datasets.
<ul>
    <li><a href="https://climatedata.imf.org/datasets/4063314923d74187be9596f10d034914/explore">Annual Surface Temperature Change</a></li>
    <li><a href="https://climatedata.imf.org/datasets/b13b69ee0dde43a99c811f592af4e821/explore">Climate-related Disasters Frequency</a></li>
</ul>
The extensive data points found in these datasets enabled me to make detailed analyses.
</br></br>
To take full advantage of the datasets in this instance, I created and executed a data pipeline <a href="https://github.com/tanvirtanjum/MADE-SS-24/blob/main/project/pipeline.py">(Source Code)</a> to help structure a workflow for the data back end. <div align="center"><img src="./ETL.png" style="max-width:60%; height:auto" alt="ETL"><br><small><em><b>Figure 1: ETL Pipeline</b></em></small></small></div>
Creating this pipeline was an important part of the project. Firstly, it was used to fetch the data from the source. <div align="center" style="width: 100vw"><div align="center" style="width:50%"><img src="./Annual Surface Temperature Change.png" style="max-width:100%; height:auto" alt="Annual Surface Temperature Change"><br><small><em><b>Figure 2: DB - Source 1</b></em></small></small></div><div align="center" style="width:50%"><img src="./" style="max-width:100%; height:auto" alt=""><br><small><em><b>Figure 1: ETL Pipeline</b></em></small></small></div></div> Then it was used to sort out the work conducted by merging and creating a singular format for data transference and analysis. Some of the steps included in the process were data cleaning, transformation, and validation to ensure that all of the steps were in order and that the data flow was reliable. By doing this, I was able to greatly enhance the quality of the research while showing how important data engineering is to any research project.

</div>

> ##### 3. Analysis

> ##### 4. Conclusions
