#!/usr/bin/env python
# coding: utf-8

# # Flight Analysis
# ### How big is the impact of a delay?

# In[1]:


import findspark
findspark.init()


# In[2]:


findspark.find()
import pyspark
findspark.find()


# In[3]:


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = pyspark.SparkConf().setAppName('appName').setMaster('local[4]')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)


# In[4]:


from pyspark.sql.functions import col, broadcast, when, mean, round, count, countDistinct, desc, asc, first, lit, bround, corr, avg
from pyspark.sql.types import IntegerType, FloatType, DoubleType
from IPython.display import display, Markdown


# ## Introduction to the Flights dataset

# According to a 2010 report made by the US Federal Aviation Administration, the economic price of domestic flight delays entails a yearly cost of 32.9 billion dollars to passengers, airlines and other parts of the economy. More than half of that amount comes from passengers' pockets, as they do not only waste time waiting for their planes to leave, but also miss connecting flights, spend money on food and have to sleep on hotel rooms while they're stranded.
# 
# The report, focusing on data from year 2007, estimated that air transportation delays put a 4 billion dollar dent in the country's gross domestic product that year. Full report can be found 
# <a href="http://www.isr.umd.edu/NEXTOR/pubs/TDI_Report_Final_10_18_10_V3.pdf">here</a>.
# 
# But which are the causes for these delays?
# 
# In order to answer this question, we are going to analyze the provided dataset, containing up to 1.936.758 different internal flights in the US for 2008 and their causes for delay, diversion and cancellation; if any.
# 
# The data comes from the U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics (BTS)
# 
# This dataset is composed by the following variables:
# 1. **Year** 2008
# 2. **Month** 1
# 3. **DayofMonth** 1-31
# 4. **DayOfWeek** 1 (Monday) - 7 (Sunday)
# 5. **DepTime** actual departure time (local, hhmm)
# 6. **CRSDepTime** scheduled departure time (local, hhmm)
# 7. **ArrTime** actual arrival time (local, hhmm)
# 8. **CRSArrTime** scheduled arrival time (local, hhmm)
# 9. **UniqueCarrie**r unique carrier code
# 10. **FlightNum** flight number
# 11. **TailNum** plane tail number: aircraft registration, unique aircraft identifier
# 12. **ActualElapsedTime** in minutes
# 13. **CRSElapsedTime** in minutes
# 14. **AirTime** in minutes
# 15. **ArrDelay** arrival delay, in minutes: A flight is counted as "on time" if it operated less than 15 minutes later the scheduled time shown in the carriers' Computerized Reservations Systems (CRS).
# 16. **DepDelay** departure delay, in minutes
# 17. **Origin** origin IATA airport code
# 18. **Dest** destination IATA airport code
# 19. **Distance** in miles
# 20. **TaxiIn** taxi in time, in minutes
# 21. **TaxiOut** taxi out time in minutes
# 22. **Cancelled** *was the flight cancelled
# 23. **CancellationCode** reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
# 24. **Diverted** 1 = yes, 0 = no
# 25. **CarrierDelay** in minutes: Carrier delay is within the control of the air carrier. Examples of occurrences that may determine carrier delay are: aircraft cleaning, aircraft damage, awaiting the arrival of connecting passengers or crew, baggage, bird strike, cargo loading, catering, computer, outage-carrier equipment, crew legality (pilot or attendant rest), damage by hazardous goods, engineering inspection, fueling, handling disabled passengers, late crew, lavatory servicing, maintenance, oversales, potable water servicing, removal of unruly passenger, slow boarding or seating, stowing carry-on baggage, weight and balance delays.
# 26. **WeatherDelay** in minutes: Weather delay is caused by extreme or hazardous weather conditions that are forecasted or manifest themselves on point of departure, enroute, or on point of arrival.
# 27. **NASDelay** in minutes: Delay that is within the control of the National Airspace System (NAS) may include: non-extreme weather conditions, airport operations, heavy traffic volume, air traffic control, etc.
# 28. **SecurityDelay** in minutes: Security delay is caused by evacuation of a terminal or concourse, re-boarding of aircraft because of security breach, inoperative screening equipment and/or long lines in excess of 29 minutes at screening areas.
# 29. **LateAircraftDelay** in minutes: Arrival delay at an airport due to the late arrival of the same aircraft at a previous airport. The ripple effect of an earlier delay at downstream airports is referred to as delay propagation

# Read the CSV file using Spark's default delimiter (","). The first line contains the headers so it is not part of the data. Hence we set the header option to true.

# In[5]:


# This does nothing: Spark is lazy so the read operation will be deferred until an action is executed
flightsDF = spark.read.option("header", "true").csv("flights_jan08.csv")

# import dataset with airports and states
# adapted from data downlaoded here: https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236
airportsStatesDF = spark.read.csv('airports_states.csv', header = True)
airportsStatesDF.withColumnRenamed('ORIGIN', 'Origin')

# import dataset with states and regions
# created ourselves with FAA information found here: https://www.faa.gov/airports/
statesRegionsDF = spark.read.csv('states_regions.csv', sep = ';', header = True)

# join datasets
flightsDF = flightsDF.join(broadcast(airportsStatesDF), on = 'Origin', how = 'left')
flightsDF = flightsDF.join(broadcast(statesRegionsDF), flightsDF.ORIGIN_STATE_ABR == statesRegionsDF.State, how = 'left')

# cache new dataframe
flightsDF.cache()


# ## Your topic: components of the delay
# <font color = blue> Delays create a huge cost to the airline industry. According to a report commissioned in 2008, the costs of delays are (source: https://www.airlines.org/data/annual-u-s-impact-of-flight-delays-nextor-report/):
#     
# - 8.3 billion USD to airlines for maintenance, crew and fuel
# - 16.7 billion USD to passengers for missed/canceled flights and delays
# - 2.2 billion USD of welfare loss due to passengers avoiding flights
# - 4 billion USD damage to United States GDP
# 
# These numbers show why there is a significant need for the airline industry and other stakeholders such as airports and the overseeing government agencies to take measures to reduce flight delays. 
# 
# In order to be able to tackle this problem, the airline industry needs to find patterns and insights in order to understand the problem so that they can derive concrete measures to solve it. These insights include the most effected regions and airports, the effect of certain types of delays and what flights (long or short distance) are effected most. For these reasons, the following analysis is highly relevant to the airline industry, as these insights can lead to major cost reductions. </font>

# ## Dataset Creation
# <font color=blue> To answer the given questions, we needed a dataset that maps IATA codes to regions. We weren't able to directly find such a dataset, so we built two datasets where the first (airports_states.csv) maps IATA codes to states and the second (states_regions.csv) maps states to regions.
# 
# The first dataset we created by downloading a large flights dataset like the one used in this analysis. However, this dataset had columns on the states of the airport. We then just kept the columns for IATA code and state, and because the the dataset had a few hundred thousand rows, we got the distinct values to simplify it. The first dataset was downloaded here: https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236
# 
# <br>The second dataset we built ourselves using Microsoft Excel because it only has 50 rows, one for every state. We couldn't find a dataset mapping states to regions, but on the FAA website they have their own classification of official regions they consider. Due to the small size, it was easy to create our own csv file with that information. The description used to build the second dataset can be found here: https://www.faa.gov/airports/
# 
# Both csv files we used are delivered with this submission. </font>

# We want to check the different kinds of delays (at origin, due to weather, security, NAS, etc) and which airports are more prone to each of those. For that purpose, **you have to get (or build yourself) a small dataset with region where each each airport is located** in terms of geographical areas of the US, such as *west coast, north-west, south-west, center, east-coast, north-east*, etc. Once you have it, answer the following questions:
# 
# 1. Is there a relation between the geographical area and the proportion of flights departing from it suffering from each type of delay? Can we say some airports are more prone to certain types of delay?
# 2. Which airports are more likely to cause security delays when a flight departs from them? What about the destination airport?
# 3. Could you say what types of delay (weather, NAS, Security, carrier, etc) show up together most often? Compute correlations of those columns *separately per airport* using function `F.corr` as the aggregation function. What's the reason for this?
# 4. Are the different types of delay related to the flight distance? Study it separately.
# 5. For delays not related to weather, are they more common during the week or at weekends? Compute the average of each delay type during the week and at weekends. 
# 6. Is the departure delay recoverable? How often a flight with a departure delay arrived with the same or smaller arrival delay? How does it relate with the flight distance? Are longer flights more prone to absorbing a departure delay and arrive on time? 
# 
# Do all analyses you deem necessary, and always support your conclusions with data!

# In[6]:


# convert delay columns to IntegerType
flightsDF = flightsDF.withColumn('CarrierDelay', col('CarrierDelay').cast(IntegerType()))                     .withColumn('NASDelay', col('NASDelay').cast(IntegerType()))                     .withColumn('SecurityDelay', col('SecurityDelay').cast(IntegerType()))                     .withColumn('LateAircraftDelay', col('LateAircraftDelay').cast(IntegerType()))                     .withColumn('WeatherDelay', col('WeatherDelay').cast(IntegerType()))


# ## 1. Is there a relation between the geographical area and the proportion of flights departing from it suffering from each type of delay? Can we say some airports are more prone to certain types of delay?
# 
# <font color = blue> We can see a relation between region and type of delay. From our analysis we can see that Western-Pacific has the highest arrival and departure delays among the ones where arrival delay is greater than equal to 15. We can see that airports from western pacific region have highest delays in every type of delay except for weather delays. Even though the delays are highest for Western-Pacific, they are not significantly high than other regions. </font>

# In[7]:


## Create a dataframe with total number of flights per each region.
Total_per_regionDF = flightsDF.groupBy('Region')                              .agg(count(lit(1))                              .alias('Total_flights'))                              .orderBy(col('Total_flights').desc())

## Create a dataframe with Region and all different types of delays 
DelayTypesDF = flightsDF.where((col('ArrDelay') >= 15) |                                (col('DepDelay') > 0) |                                (col('CarrierDelay') > 0) |                                (col('WeatherDelay') > 0) |                                (col('NASDelay') > 0) |                                (col('SecurityDelay') > 0) |                                (col('LateAircraftDelay') > 0))                        .select(['Region', 'ArrDelay','DepDelay','CarrierDelay','WeatherDelay',
                                 'NASDelay','SecurityDelay','LateAircraftDelay'])
## caching the dataframes
Total_per_regionDF.cache()
DelayTypesDF.cache()

## Running a loop to create new columns *delay_present for each different type of delay
for c in ['ArrDelay','DepDelay','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay']:
    DelayTypesDF = DelayTypesDF.withColumn(c+'_present', when(col(c) > 0, lit(1)).otherwise(lit(0)))
    
# Selecting samples where there is at least one type of delay before running a loop for generating columns with proportions/ratios
DelayTypesDF = DelayTypesDF.where(((col('ArrDelay_present').cast(IntegerType())) >= 15)|                (col('DepDelay_present')>0)|                (col('CarrierDelay_present')>0)|                (col('WeatherDelay_present')>0)|                (col('NASDelay_present')>0)|                (col('SecurityDelay_present')>0)|                (col('LateAircraftDelay_present')>0)).select(['Region',
                                                           'ArrDelay_present',
                                                           'DepDelay_present',
                                                           'CarrierDelay_present',
                                                           'WeatherDelay_present',
                                                           'NASDelay_present',
                                                           'SecurityDelay_present',
                                                           'LateAircraftDelay_present'])

DelayCountsDF = DelayTypesDF.groupBy('Region').sum()

newDF = Total_per_regionDF.join(DelayCountsDF,"Region")
newDF.cache()
finalDF = newDF.select(newDF.columns)
finalDF.cache()

## Running a loop to generate columns with ratios of each different type of delay
for c in ['sum(ArrDelay_present)','sum(DepDelay_present)','sum(CarrierDelay_present)','sum(WeatherDelay_present)','sum(NASDelay_present)','sum(SecurityDelay_present)','sum(LateAircraftDelay_present)']:
    finalDF = finalDF.withColumn('proportion_of_'+c,(col(c).cast(DoubleType())/col('Total_flights').cast(DoubleType())))

finalDF.select('Region',
              bround(col('proportion_of_sum(ArrDelay_present)'),3).alias('ratio_ArrD'),
              bround(col('proportion_of_sum(DepDelay_present)'),3).alias('ratio_DepD'),
              bround(col('proportion_of_sum(CarrierDelay_present)'),3).alias('ratio_CarrierD'),
              bround(col('proportion_of_sum(WeatherDelay_present)'),3).alias('ratio_WeaD'),
              bround(col('proportion_of_sum(NASDelay_present)'),3).alias('ratio_NASD'),
              bround(col('proportion_of_sum(SecurityDelay_present)'),3).alias('ratio_SecD'),
              bround(col('proportion_of_sum(LateAircraftDelay_present)'),3).alias('ratio_LateACD')).show()


# ## 2. Which airports are more likely to cause security delays when a flight departs from them? What about the destination airport?
# <font color=blue> Security delays cause very negative experiences to travelers, since they are related to evacuation of terminals, re-boarding of aircraft, among others. The analysis aims to show the likelihood of security delays considering the number of incidents divided by the total amount of flights. The airports that had the highest number of incidents for security delays both as an arrival and departure is Las Vegas airport. It is interesting to notice that top 10 list of flights is the same both for origin and destination, with a slight difference that Phoenix airport is more likely to cause security delay as a destination airport, while Chicago Midway is more likely as an origin airport. </font>

# In[8]:


# Which airports are more likely to cause security delays when a flight departs from them? What about the destination airport?

print ("Top 10 airports that are most likely to cause security delays based on origin:")

TotalFlights = flightsDF.count()

Delayed_by_origin = flightsDF.where(~col("SecurityDelay").isNull())       .select("Origin","SecurityDelay")       .groupBy("Origin")       .agg(count("Origin").alias("NumFlights"), (count("SecurityDelay") / TotalFlights * 100).alias("RatioO"))       .select("Origin", "NumFlights", round("RatioO",2).alias("Ratio"))       .orderBy(col("Ratio").desc())       .show(10)

print ("Top 10 airports that are most likely to cause security delays based on destination:")

Delayed_by_destination=flightsDF.where(~col("SecurityDelay").isNull())       .select("Dest","SecurityDelay")       .groupBy("Dest")       .agg(count("Dest").alias("NumFlights"), (count("SecurityDelay") / TotalFlights * 100).alias("RatioD"))       .select("Dest","NumFlights",round("RatioD",2).alias("Ratio"))       .orderBy(col("Ratio").desc()).show(10)


# ## 3. Could you say what types of delay (weather, NAS, Security, carrier, etc) show up together most often? Compute correlations of those columns separately per airport using function F.corr as the aggregation function. What's the reason for this?
# <font color=blue> There are two types of delay which occur quite often compared to the other: Carrier Delay with NAS delay, and NAS delay with Late Aircraft delay. These two happened together almost 1000 each. Some types of delays, such as Carrier with Security, Carrier with Weathter and Security with Weather did not occur together at all considering the 15 minute benchmark we used to consider a delay.
# 
# For these two common delays, computing their correlation for each of the airports in the dataset we found that the correlation was negative for the vast majority of observations. The reason for this is that, even though
# some of these delays occurred together a few thousand times, it is still a low proportion considering the dataset
# has over 100,000 flights. In general it is not very likely that there will be two types of delays together and
# that is why there is a negative correlation </font>

# In[9]:


# count the co-occurences of the delay types
flightsDF.agg(count(when(((col("CarrierDelay") >= 15) & (col("NASDelay") >= 15)), True)).alias("Ca-NAS"),
    count(when(((col("CarrierDelay") >= 15) & (col("SecurityDelay") >= 15)), True)).alias("Ca-Sec"),
    count(when(((col("CarrierDelay") >= 15) & (col("LateAircraftDelay") >= 15)), True)).alias("Ca-Late"),
    count(when(((col("CarrierDelay") >= 15) & (col("WeatherDelay") >= 15)), True)).alias("Ca-Wea"),
    count(when(((col("NASDelay") >= 15) & (col("SecurityDelay") >= 15)), True)).alias("NAS-Sec"),
    count(when(((col("NASDelay") >= 15) & (col("LateAircraftDelay") >= 15)), True)).alias("NAS-Late"),
    count(when(((col("NASDelay") >= 15) & (col("WeatherDelay") >= 15)), True)).alias("NAS-Wea"),
    count(when(((col("SecurityDelay") >= 15) & (col("WeatherDelay") >= 15)), True)).alias("Sec-Wea"),
    count(when(((col("SecurityDelay") >= 15) & (col("LateAircraftDelay") >= 15)), True)).alias("Sec-Late"),
    count(when(((col("LateAircraftDelay") >= 15) & (col("WeatherDelay") >= 15)), True)).alias("Late-Weather")).show()

# get the correlation of the two most common co-occurences for each airport
flightsDF.groupBy("Origin")         .agg(round(corr("CarrierDelay","NASDelay"), 2).alias("Carrier-LateAircraft"),
              round(corr("NASDelay","LateAircraftDelay"),2).alias("NAS-LateAircraft"))\
         .where((col("Carrier-LateAircraft").isNotNull()\
                 & (col("NAS-LateAircraft") !="NaN")\
                 & (col("Carrier-LateAircraft") !="NaN")))\
         .show(200)


# ## 4. Are the different types of delay related to the flight distance? Study it separately.
# <font color = blue> The flights were categorized in 4 different categories based on distance:
# 
# - Very short --> flights between 0 and 500 miles.
# - Short --> Flight between 500 and 1100 miles.
# - Medium --> Flights between 1100 and 1700 miles.
# - Long --> Higher than 1700 miles.
# 
# The interpretation of the distance can be perceived at first glance as not affecting the amount of delays. The ratio of delays based only on distance has a range from 17.2 % to 21.2 %. The highest ratio of delays is Very Short Distance Category followed by Long Distance with 20.7 %. As we can see the top two ratios are the opposites regarding flight distance. Considering this it doesnt seem to be a strong correlation between delays and distance in a flight.
# 
# For further examination the different types of delays were analyzed to obtain a better understanding of how distance may affect each type of delay. Analysis of 5 different delay types was performed and the following analysis was made:
# 
# The very short distance is the highest ratio of all categories this is due to the amount of flights in this category.
# NAS Delay has the highest delays compared to all 5 types of delays in very short distance. This is due to air traffic control on busy airports or traffic on take off. The longer the flight the less amount of delay it presents since you can make up time during longer flights to compensate.
# 
# Security Delay has the highest ratio of all types in Short distance flights. As a passenger it seems you are most likely to see a security breach in general in this type of flights. A good thing about this type of delay is that is usually the shortest type of delay reagarding time.
# 
# Weather Delay seems to be affect more in long distance flights in other categories it stays in the middle of the pack compared to all the other delay types. So the longer the flight the greater risk of running into a Weather Delay. Another thing to consider with Weather Delays is they produce the longest delays of all types.
# Late Aircraft Delay is present more in Very Short and Short distances compared to other types. this may be due to the amount of times a route is flown per day. In this short distance flights the same route can be flown during the same day producing delays towards the end on the day since all minimal delays add up. Also Late Aircraft Delays are the most common type of delay.
# Carrier Delay is the highest for Medium distance flights. You can expect this type of delay in flights that last between 2 and 3 hours of flight time. </font>

# In[10]:


# Categorizing distance column into 4 categories: very short (93 to 500 miles), short(500 to 1100 miles), 
# medium (1100 to 1700 miles) and long (Above 1700 miles). 
flightsDF = flightsDF.withColumn("Distance_Category", when((col("Distance")>=93) & (col("Distance")<=500), "1.Very Short")                                                     .when((col("Distance")>500) & (col("Distance")<=1100), "2.Short")                                                     .when((col("Distance")>1100) & (col("Distance")<=1700), "3.Medium")                                                     .otherwise("4.Long"))


delayed_flights = flightsDF.filter(col('ArrDelay') >= 15)

# Summary of Distance
delayed_flights.select('Distance').summary().show()

totalFlights = flightsDF.count()
totaldelayedflights = delayed_flights.count()
totalVS = flightsDF            .where(col("Distance_Category") == "1.Very Short")            .count()

totalS = flightsDF            .where(col("Distance_Category") == "2.Short")            .count()

totalMD = flightsDF            .where(col("Distance_Category") == "3.Medium")            .count()

totalLO = flightsDF            .where(col("Distance_Category") == "4.Long")            .count()

print("** Total Number of Flights by Distance Category ** :")
flightsDF.groupBy("Distance_Category")         .agg(count("Distance_Category").alias("#Flights"))         .orderBy("Distance_Category").show()

print("** Ratio of Delayed Flights on VERY SHORT Distance ** :")
delayed_flights.where(col("Distance_Category") == "1.Very Short")            .agg(count("Distance_Category").alias("#DelayedFlights"),                (round((count("Distance_Category")/totalVS*100),1).alias("Ratio")))            .show()

print("** Ratio of Delayed Flights on SHORT Distance ** :")
delayed_flights.where(col("Distance_Category") == "2.Short")            .agg(count("Distance_Category").alias("#DelayedFlights"),                (round((count("Distance_Category")/totalS*100),1).alias("Ratio")))            .show()

print("** Ratio of Delayed Flights on MEDIUM Distance ** :")
delayed_flights.where(col("Distance_Category") == "3.Medium")            .agg(count("Distance_Category").alias("#DelayedFlights"),                (round((count("Distance_Category")/totalMD*100),1).alias("Ratio")))            .show()

print("** Ratio of Delayed Flights on LONG Distance ** :")
delayed_flights.where(col("Distance_Category") == "4.Long")            .agg(count("Distance_Category").alias("#DelayedFlights"),                (round((count("Distance_Category")/totalLO*100),1).alias("Ratio")))            .show()

totalNAS = delayed_flights            .where(col("NASDelay") > 0)            .count()

totalSec = delayed_flights            .where(col("SecurityDelay") > 0)            .count()

totalWea = delayed_flights            .where(col("WeatherDelay") > 0)            .count()

totalLAD = delayed_flights            .where(col("LateAircraftDelay") > 0)            .count()

totalCar = delayed_flights            .where(col("CarrierDelay") > 0)            .count()

print("** Delayed flights NAS DELAY per Distance Category ** :")
delayed_flights.where(col("NASDelay")> 0)                     .select("Distance_Category", "NASDelay","ArrDelay")                     .groupBy("Distance_Category")                     .agg(count("Distance_Category").alias("NumFlights"),                           (count("Distance_Category")/totalNAS*100).alias("Ratio"),                          (round(avg("ArrDelay"),1)).alias("AvgDelay(min)"))                     .orderBy("Distance_Category")                     .select("Distance_Category","NumFlights","AvgDelay(min)",round("Ratio",1).alias("Ratio")).show()

print("** Delayed flights SECURITY DELAY per Distance Category ** :")
delayed_flights.where(col("SecurityDelay")> 0)                     .select("Distance_Category", "SecurityDelay","ArrDelay")                     .groupBy("Distance_Category")                     .agg(count("Distance_Category").alias("NumFlights"),                           (count("Distance_Category")/totalSec*100).alias("Ratio"),                          (round(avg("ArrDelay"),1)).alias("AvgDelay(min)"))                     .orderBy("Distance_Category")                     .select("Distance_Category","NumFlights","AvgDelay(min)",round("Ratio",1).alias("Ratio")).show()

print(" ** Delayed flights WEATHER DELAY per Distance Category ** :")
delayed_flights.where(col("WeatherDelay")> 0)                     .select("Distance_Category", "WeatherDelay","ArrDelay")                     .groupBy("Distance_Category")                     .agg(count("Distance_Category").alias("NumFlights"),                           (count("Distance_Category")/totalWea*100).alias("Ratio"),                          (round(avg("ArrDelay"),1)).alias("AvgDelay(min)"))                     .orderBy("Distance_Category")                     .select("Distance_Category","NumFlights","AvgDelay(min)",round("Ratio",1).alias("Ratio")).show()

print(" ** Delayed flights LATE AIRCRAFT DELAY per Distance Category ** :")
delayed_flights.where(col("LateAircraftDelay")> 0)                     .select("Distance_Category", "LateAircraftDelay","ArrDelay")                     .groupBy("Distance_Category")                     .agg(count("Distance_Category").alias("NumFlights"),                           (count("Distance_Category")/totalLAD*100).alias("Ratio"),                          (round(avg("ArrDelay"),1)).alias("AvgDelay(min)"))                     .orderBy("Distance_Category")                     .select("Distance_Category","NumFlights","AvgDelay(min)",round("Ratio",1).alias("Ratio")).show()

print("** Delayed flights CARRIER DELAY per Distance Category ** :")
delayed_flights.where(col("CarrierDelay")> 0)                     .select("Distance_Category", "CarrierDelay","ArrDelay")                     .groupBy("Distance_Category")                     .agg(count("Distance_Category").alias("NumFlights"),                           (count("Distance_Category")/totalCar*100).alias("Ratio"),                          (round(avg("ArrDelay"),1)).alias("AvgDelay(min)"))                     .orderBy("Distance_Category")                     .select("Distance_Category","NumFlights","AvgDelay(min)",round("Ratio",1).alias("Ratio")).show()


# ## 5. For delays not related to weather, are they more common during the week or at weekends? Compute the average of each delay type during the week and at weekends.
# 
# <font color=blue> We can see that the percentage of flights delayed is higher on the weekends (21.7%) than on weekdays (19.0%). The second dataframe also shows that amongst delayed flights (flights with ArrDelay > 15), the average delay is longer in each type of delay. What can explain that all type of delays occur more often?
#     
# One hypothesis we had is that is that on weekends the operations are more strained. Since flights is a tight knit transportation network with a lot of interdependencies, if flights start to get delayed it can cascade to the following flights. This can impact airline operations (CarrierDelay), airport operations (SecurityDelay) and control operations 
# (NASDelay).
# 
# <br>The dataframe printed for reference shows that there are in total less flights on weekends (day 6 & 7) than on weekdays. This would make operations easier for all stakeholders involved. However, it can also be assumed that on weekends the personnel is reduced significantly. This information is not available in the dataset, but it can explain why on weekends all types of delays are longer on average. With less personnel, it is more challenging for each stakeholder to complete the tasks in time and less likely that they will catch up once they have fallen behind.
# 
# In conclusion, airlines, airports and administrative authorities need to benchmark their weekend operations against their weekday operations to find the exact cause of the longer average delays on weekends, because there is a clear difference that can be identified here. </font>

# In[11]:


# show number of flights on each day of the week for reference
flightsDF.groupBy('dayofweek').count().orderBy(asc('dayofweek')).show()

# create a column showing if the day is Weekend or Weekday
flightsDF = flightsDF.withColumn('Weekend', when(col('DayOfWeek') == 6, 'Weekend')                                            .when(col('DayOfWeek') == 7, 'Weekend')                                            .otherwise('Weekday'))

# create a column showing whether a flight is delayed or not
flightsDF = flightsDF.withColumn('Delayed', when(col('ArrDelay') >= 15, 1).otherwise(0))

# get the percentage of delayed flights on weekends and weekdays
flightsDF.groupBy('Weekend').agg(mean('Delayed').alias('MeanDelayed'))                            .select('Weekend', round(100 * col('MeanDelayed'), 1).alias('Percentage Delayed'))                            .show()
    
# get a dataframe with only delays
delayedFlightsDF = flightsDF.filter(col('Delayed') == 1)

# create a dataframe with only delays not related to weather
nonWeatherDelays = delayedFlightsDF.filter(col('WeatherDelay') == 0)

# calculate the average of the four delay types on weekends versus weekdays
nonWeatherDelays.groupBy('Weekend').agg(mean('CarrierDelay').alias('MeanCarrierDelay'),
                                        mean('NASDelay').alias('MeanNASDelay'),
                                        mean('SecurityDelay').alias('MeanSecurityDelay'),
                                        mean('LateAircraftDelay').alias('MeanLateAircraftDelay'))\
                                   .select('Weekend',
                                           round('MeanCarrierDelay', 2).alias('Average Carrier Delay (min)'),
                                           round('MeanNASDelay', 2).alias('Average NAS Delay (min)'),
                                           round('MeanSecurityDelay', 2).alias('Average Security Delay (min)'),
                                           round('MeanLateAircraftDelay', 2).alias('Average Late Aircraft Delay (min)'))\
                                   .show()


# ## 6. Is the departure delay recoverable? How often a flight with a departure delay arrived with the same or smaller arrival delay? How does it relate with the flight distance? Are longer flights more prone to absorbing a departure delay and arrive on time?
# 
# <font color = blue> From our analysis we can conclude that the delay recovery is possible, but happens mostly in the short haul flights than in the long haul flights. This is very counter intuitive, because general expectation would be that if the distance of the journey is longer, there is more time for recovering the lost time. But that does not seem to happen so in this case. Instead of taking the absolute numbers for identifying whether long haul or short haul recovers faster, we took the proportion of short/long haul flights that have a departure delay > 0 and then took a ratio of the ones which have arrival delay lower than departure delay. This gives us a better estimate of the proportional recovery. </font>

# In[12]:


flightsDF.select(col('Distance').cast(IntegerType())).summary().show()

## Considering distance more than 1700 as long distance
flightsHaulDF = flightsDF.withColumn("Long_Short_Haul",when((col('Distance').cast(IntegerType())) >= 1700,'Long').otherwise('Short'))

## Calculating the short haul and long haul total flights to calculate the ratio later
short_total = flightsHaulDF.where(col('Long_Short_Haul') == 'Short').count()
long_total = flightsHaulDF.where(col('Long_Short_Haul') != 'Short').count()

## Calculating the percentage of flights that have recoverable delay after taking in the entries which have more 
## DepDelay than ArrDelay among all the entries with DepDelay > 0
flightsHaulDF.where(col('DepDelay').cast(FloatType())>0)             .where(col("Long_Short_Haul") == "Short")             .where(col('ArrDelay') <= col('DepDelay'))             .groupBy("Long_Short_Haul").agg(count(lit(1)).alias("Num_short"))             .withColumn("percentage_of_flights_with_recovered_delays",col('Num_short')/short_total*100).show()


flightsHaulDF.where(col('DepDelay').cast(FloatType())>0)             .where(col("Long_Short_Haul") == "Long")             .where(col('ArrDelay') <= col('DepDelay'))             .groupBy("Long_Short_Haul").agg(count(lit(1)).alias("Num_long"))             .withColumn("percentage_of_flights_with_recovered_delays",col('Num_long')/short_total*100).show()

## Considering distance more than 1100 as long distance
flightsHaulDF = flightsDF.withColumn("Long_Short_Haul",when((col('Distance').cast(IntegerType()))>= 1100,'Long').otherwise('Short'))

## Calculating the short haul and long haul total flights to calculate the ratio later
short_total = flightsHaulDF.where(col('Long_Short_Haul') == 'Short').count()
long_total = flightsHaulDF.where(col('Long_Short_Haul') != 'Short').count()

## Calculating the percentage of flights that have recoverable delay after taking in the entries which have more 
## DepDelay than ArrDelay among all the entries with DepDelay > 0
flightsHaulDF.where(col('DepDelay').cast(FloatType())>0)             .where(col("Long_Short_Haul") == "Short")             .where(col('ArrDelay') <= col('DepDelay'))             .groupBy("Long_Short_Haul").agg(count(lit(1)).alias("Num_short"))             .withColumn("percentage_of_flights_with_recovered_delays",col('Num_short')/short_total*100).show()


flightsHaulDF.where(col('DepDelay').cast(FloatType())>0)             .where(col("Long_Short_Haul") == "Long")             .where(col('ArrDelay') <= col('DepDelay'))             .groupBy("Long_Short_Haul").agg(count(lit(1)).alias("Num_long"))             .withColumn("percentage_of_flights_with_recovered_delays",col('Num_long')/short_total*100).show()


# In[ ]:




