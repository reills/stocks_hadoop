# Stock Analysis Project

This project provides various MapReduce methods to analyze stock market data. 
These include looking at stock price, trends, and trading volume.

---

## Available techniques

### 1. MinMax Analysis
Located in `minmax/MinMaxAnalysis.java`:
- Computes the minimum and maximum closing values of stocks by symbol.
- Performs this analysis both over time and for the entire input file.

### 2. Trending Analysis
Located in `trending/TrendingAnalysis.java`:
- Calculates the average daily change in closing prices for each stock symbol.
- Defaults to analyzing the last 30 days of data.
- Useful for comparing the performance of multiple stocks over the same period.

### 3. Biggest Mover Analysis
Located in `biggestmoveranalysis/BiggestMoverAnalysis.java`:
- Identifies the top stock mover (highest percentage gain) for each trading day.
- Based on the opening and closing prices.

### 4. Volume Average Analysis
Located in `volume/VolumeAverageAnalysis.java`:
- Calculates the average dollar volume traded per day for each stock.
- Formula: ```Average Daily Dollar Volume = Total Dollar Volume / Number of Days Dollar Volume = Volume × Closing Price ```

### 4. Volume Average Analysis
Located in `volume/VolumeAverageAnalysis.java`:
- Calculates the average dollar volume traded per day for each stock.
- Formula: ```Average Daily Dollar Volume = Total Dollar Volume / Number of Days Dollar Volume = Volume × Closing Price ```


### 5. Yearly Top Performer
Located in `yearlytop/TopPerformer.java`:
- Computes the performance of each stock over the last 'x' days (default is 365, representing a year).
- Outputs: - The start date of the period,t he percentage gain for each stock, calculated using the formula:  
  ```
  Percentage Gain = ((Last Close - First Close) / First Close) * 100
  ```


## Running program
### Before Running the Project

1. **Stop any stale services:**
 - ```stop-dfs.sh```
 - ```stop-yarn.sh```
2. **Start services**
 - ```start-dfs.sh```
 - ```start-yarn.sh```
3. **Check for jps**
 - ```jps```
should see: NameNode, DataNode, ResourceManager, NodeManager

### Run project
 ```./run.sh```

### Trouble shoot
- check disk space: ```df -h```
- clear out temporary hdfs files: ```hdfs dfs -rm -r /tmp/*```
- remove log files: ```rm -rf $HADOOP_HOME/logs/*```


## Checking Results

### Check output directories
```hdfs dfs -ls output```
should see-- min_max, volume, movers, trending folder
### Reducer is one so results are here
- ```hdfs dfs -cat output/min_max/part-r-00000```
- ```hdfs dfs -cat output/volume/part-r-00000```
- ```hdfs dfs -cat output/movers/part-r-00000```
- ```hdfs dfs -cat output/trending/part-r-00000```
### Reset folder (deletes output and all subfolders )
- ```hdfs dfs -rm -r output```



