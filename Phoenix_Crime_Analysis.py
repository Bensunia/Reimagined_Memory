#!/usr/bin/env python
# coding: utf-8

# # Phoenix Crime Analysis
# 1. Setting up Spark Environment.
# 2. Connecting to Dataset.
# 3. Analysis of Dataset.
#     3.1 Infer schema and total row counter.
#     3.2 Identifying Data.
#     3.3 Profiling Columns.
# 4. Columns Basic Analysis
#     4.1 Time Columns
#     4.2 Crime Columns
#     4.3 Location Columns
# 5. Business questions to improve reduce crime in Phoenix.
#     5.1 Top 10 Zip Codes by crime category.
#     5.2
#     5.3 Analysis of the ZIP codes that have the highest % of violent crimes.

# ## 1. Setting up Spark environment

# In[2]:


import findspark
findspark.init()

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


# ## 2. Connecting to Dataset. Creating a Data Frame

# In[3]:


crime_DF = spark.read
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("Phoenix_Crime.csv")


# ## 3. Analysis of data set
# ### 3.1 Infer schema and total row counter

# In[4]:


from IPython.display import *
from pyspark.sql.functions import *

crime_DF.printSchema()
display(Markdown("This DataFrame has **%d rows**." % crime_DF.count()))


# ### 3.2 Identifying Data
# - **Entities: Crimes, Zip Code, Type of Establishment.**
# - **Metrics: Stamp Times for the crime in and out.**
# - **Dimensions: Addressess, Unique Identifier of Crime.**
#
# ######## Unique identifier wont be taken into account during analysis.

# ### 3.3 Profiling Columns
# - **Time Columns: OCCURRED ON, OCCURRED TO.**
# - **Crime Columns: UCR CRIME CATEGORY, PREMISE TYPE.**
# - **Location Columns: 100 BLOCK ADDR, ZIP.**

# ## 4. Columns Basic Analysis
# ### 4.1 Time Columns Analysis

# In[13]:


## Time Columns Analysis
from IPython.display import display, Markdown
from pyspark.sql.functions import when, count, col, countDistinct, desc, first, lit

print("Checking for nulls on columns Occurred On, Occurred To:")
crime_DF.select([count(when(col(c).isNull(), c)).alias(c) for c in ["OCCURRED ON","OCCURRED TO"]]).show()

print("Checking amount of distinct values in columns Occurred On, Occurred To:")
crime_DF.select([countDistinct(c).alias(c) for c in ["OCCURRED ON","OCCURRED TO"]]).show()

print ("Most and least frequent occurrences for Occured On and Occurred To:")
OccurredONDF = crime_DF.groupBy("OCCURRED ON").agg(count(lit(1)).alias("Total"))
OccurredTODF = crime_DF.groupBy("OCCURRED TO").agg(count(lit(1)).alias("Total"))

leastFreqOCCON    = OccurredONDF.orderBy(col("Total").asc()).show(5)
mostFreqOCCON     = OccurredONDF.orderBy(col("Total").desc()).show(5)
leastFreqOCCTO     = OccurredTODF.orderBy(col("Total").asc()).show(5)
mostFreqOCCTO      = OccurredTODF.orderBy(col("Total").desc()).show(5)


# In[33]:


crime_DF.orderBy(col("OCCURRED ON").asc()).show(10)


# ### 4.2 Crime Columns Analysis

# In[6]:


### Crime Columns Analysis
from IPython.display import display, Markdown
from pyspark.sql.functions import when, count, col, countDistinct, desc, first, lit

print("Checking for nulls on columns UCR CRIME CATEGORY, PREMISE TYPE:")
crime_DF.select([count(when(col(c).isNull(), c)).alias(c) for c in ["UCR CRIME CATEGORY","PREMISE TYPE"]]).show()

print("Checking amount of distinct values in columns UCR CRIME CATEGORY, PREMISE TYPE:")
crime_DF.select([countDistinct(c).alias(c) for c in ["UCR CRIME CATEGORY","PREMISE TYPE"]]).show()

print ("Most and least frequent occurrences for CRIME and PREMISE:")
CrimeOccurrencesDF = crime_DF.groupBy("UCR CRIME CATEGORY").agg(count(lit(1)).alias("Total"))
PremiseDF = crime_DF.groupBy("PREMISE TYPE").agg(count(lit(1)).alias("Total"))

leastOccurrencesCrime    = CrimeOccurrencesDF.orderBy(col("Total").asc()).first()
mostOccurrencesCrime     = CrimeOccurrencesDF.orderBy(col("Total").desc()).first()
leastOccurrencesPremise     = PremiseDF.orderBy(col("Total").asc()).first()
mostOccurrencesPremise      = PremiseDF.orderBy(col("Total").desc()).first()

display(Markdown("""
| %s | %s | %s | %s |
|----|----|----|----|
| %s | %s | %s | %s |
""" % ("leastOccurrencesCrime", "mostOccurrencesCrime", "leastOccurrencesPremise", "mostOccurrencesPremise",\
       "%s (%d crimes)" % (leastOccurrencesCrime["UCR CRIME CATEGORY"], leastOccurrencesCrime["Total"]),\
       "%s (%d crimes)" % (mostOccurrencesCrime["UCR CRIME CATEGORY"], mostOccurrencesCrime["Total"]),\
       "%s (%d incidents)" % (leastOccurrencesPremise["PREMISE TYPE"], leastOccurrencesPremise["Total"]),\
       "%s (%d incidents)" % (mostOccurrencesPremise["PREMISE TYPE"], mostOccurrencesPremise["Total"]))))

showCrime  = CrimeOccurrencesDF.orderBy(col("Total").desc()).show()


# ### 4.3 Location Columns Analysis

# In[7]:


## Address Columns Anaylsis:
from IPython.display import display, Markdown
from pyspark.sql.functions import StringType

## Casting ZIP Column into a string.
crime_DF.withColumn("ZIP", col("ZIP").cast(StringType())).printSchema()

print("Checking for nulls on columns 100 BLOCK ADDR, ZIP:")
crime_DF.select([count(when(col(c).isNull(), c)).alias(c) for c in ["ZIP","100 BLOCK ADDR"]]).show()

print("Checking amount of distinct values in columns 100 BLOCK ADDR, ZIP:")
crime_DF.select([countDistinct(c).alias(c) for c in ["ZIP","100 BLOCK ADDR"]]).show()

print ("Most and least frequent occurrences for ZIP and 100 BLOCK ADDR:")
ZIPOccurrencesDF = crime_DF.groupBy("ZIP").agg(count(lit(1)).alias("Total"))
ADDRDF = crime_DF.groupBy("100 BLOCK ADDR").agg(count(lit(1)).alias("Total"))

print("ZIP codes with the LEAST amount of crimes:")
leastOccurrencesZIP    = ZIPOccurrencesDF.orderBy(col("Total").asc()).show(10)
print("ZIP codes with the MOST amount of crimes:")
mostOccurrencesZIP     = ZIPOccurrencesDF.orderBy(col("Total").desc()).show(10)
print("Addresses with the LEAST amount of crime:")
leastOccurrencesADDR     = ADDRDF.orderBy(col("Total").asc()).show(10)
print("Addresses with the MOST amount of crime:")
mostOccurrencesADDR      = ADDRDF.orderBy(col("Total").desc()).show(10)


# ## 5. Business questions to improve safety in Phoenix
# ### 5.1  Ratio of crimes by severity.

# In[8]:


from pyspark.sql.functions import *

# Crime severity is going to be categorized as follows (totally made up):
#
#   '4.EXTREME'= MURDER AND NON-NEGLIGENT MANSLAUGHTER , RAPE
#   '3.MAJOR'= AGGRAVATED ASSAULT, ARSON, ROBBERY
#   '2.INTERMEDIATE'= LARCENY-THEFT, MOTOR VEHICLE THEFT
#   '1.MINOR'= DRUG OFFENSE, BURGLARY

# 1. Let's enrich the DF with  crime severity based on our categorization

totalcrimes = crime_DF.count()
crimeCategoryDF = crime_DF.withColumn("CRIME SEVERITY", when((col("UCR CRIME CATEGORY")=="ARSON"),"3.MAJOR")\
                          .when((col("UCR CRIME CATEGORY")=="AGGRAVATED ASSAULT"),"3.MAJOR")\
                          .when((col("UCR CRIME CATEGORY")=="RAPE"),"4.EXTREME")\
                          .when((col("UCR CRIME CATEGORY")=="MURDER AND NON-NEGLIGENT MANSLAUGHTER"),"4.EXTREME")\
                          .when((col("UCR CRIME CATEGORY")=="LARCENY-THEFT"),"2.INTERMEDIATE")\
                          .when((col("UCR CRIME CATEGORY")=="ROBBERY"),"3.MAJOR")\
                          .when((col("UCR CRIME CATEGORY")=="MOTOR VEHICLE THEFT"),"2.INTERMEDIATE")\
                          .otherwise("1.MINOR"))

# 2. Ready to answer to this business question

print("Ratio for UCR CRIME CATEGORIES:")

crimeCategoryDF.select("UCR CRIME CATEGORY", "ZIP")\
               .groupBy("UCR CRIME CATEGORY")\
               .agg(count("UCR CRIME CATEGORY").alias("NUM CRIMES"),\
                    (count("UCR CRIME CATEGORY")/totalcrimes*100).alias("RATIO"))\
               .orderBy("UCR CRIME CATEGORY")\
               .select("UCR CRIME CATEGORY","NUM CRIMES",round("RATIO",1).alias("ROUNDED RATIO")).show()

print("Crime Severity Categories:")
print("     -'EXTREME'= MURDER AND NON-NEGLIGENT MANSLAUGHTER , RAPE")
print("     -'MAJOR' = AGGRAVATED ASSAULT, ARSON, ROBBERY")
print("     -'INTERMEDIATE' = LARCENY-THEFT, MOTOR VEHICLE THEFT")
print("     -'MINOR' = DRUG OFFENSE, BURGLARY")
print("")
print("Ratio of Crime Severity:")

crimeCategoryDF.select("CRIME SEVERITY", "ZIP")
               .groupBy("CRIME SEVERITY")
               .agg(count("CRIME SEVERITY").alias("NUM CRIMES"),\
                    (count("CRIME SEVERITY")/totalcrimes*100).alias("RATIO")) \
               .orderBy("CRIME SEVERITY")
               .select("CRIME SEVERITY","NUM CRIMES",round("RATIO",1).alias("ROUNDED RATIO")).show()


# ### 5.2 Ratio of Crimes by Top 20 Premise Type.

# In[11]:


from pyspark.sql.types import *
from pyspark.sql.functions import *

## Casting ZIP Column as STRING.
crime_DF.withColumn("ZIP", col("ZIP").cast(StringType())).printSchema()

## Generate a table with the Top 20 Premises by amount of crimes.
totalcrimes = crime_DF.count()

totalCrimesPremiseDF = crime_DF.groupBy("Premise Type") \
                               .agg(count("Premise Type").alias("TotalPremise"))

RatioPremiseDF = crime_DF.where((col("PREMISE TYPE")!="OTHER"))\
                         .select("PREMISE TYPE","UCR CRIME CATEGORY")\
                         .groupBy("PREMISE TYPE")\
                         .agg(count("PREMISE TYPE").alias("NUM CRIMES/PREMISE"),\
                            (count("PREMISE TYPE")/totalcrimes*100).alias("PREMISE RATIO"))\
                         .orderBy(col("NUM CRIMES/PREMISE").desc())\
                         .select("PREMISE TYPE","NUM CRIMES/PREMISE",round("PREMISE RATIO",1).alias("RATIO")).limit(20)

display(Markdown("**Top 20 Premise Types** with highest number of crimes and ratio (in \%):"))
RatioPremiseDF.show()


# ### 5.3  Analysis of the ZIP codes that have the highest % of violent crimes.

# In[34]:


# Our answer to this business question will be:
#   1. List of top 10 ZIP codes with highest number of crimes.
#      (based on total number of crimes)
#   2. List of top 20 ZIP codes crime ratio by severity category (major and extreme)

# In order to be able to deliver these insights, we need some preparation:
#   1. Define a DataFrame with total crimes per ZIP code (totalCrimesZIPDF)
#   2. Define a DataFrame with aggregated data by ZIP code and Crime Severity to figure out
#      number of crimes commited per crime severity  (severeCrimesDF)
#   3. Combine both DataFrames to come up with one single DataFrame containin total crimes
#      per zip code and number of crimes by crime severity to compute ratios (combinedDF)

totalCrimesZIPDF =  crime_DF.groupBy("ZIP") \
                            .agg(count("ZIP").alias("TotalCrimes"))

severeCrimesDF = crimeCategoryDF.where((col("CRIME SEVERITY")!="1.MINOR") & (col("CRIME SEVERITY")!="2.INTERMEDIATE"))\
                                .select("CRIME SEVERITY", "ZIP", "UCR CRIME CATEGORY")\
                                .groupBy("ZIP", "CRIME SEVERITY")\
                                .agg(count("CRIME SEVERITY").alias("NUM CRIME SEVERITY"))

combinedDF =   severeCrimesDF.join(totalCrimesZIPDF, "ZIP")\
                             .withColumn("SEVERITY BY ZIP RATIO", round(col("NUM CRIME SEVERITY")/col("TotalCrimes")*100,2))\
                             .where((col("TotalCrimes")>= 5))\
                             .orderBy(col("NUM CRIME SEVERITY").desc(),col("ZIP").desc())

display(Markdown("**Top 20 ZIP codes with highest number of crimes by severity ratio (in \%):**"))
combinedDF.limit(100).show(20)

display(Markdown("**Top 20 ZIP Codes with most number of crimes by severity category (in \%):**"))
combinedDF.groupBy("ZIP")\
          .pivot("CRIME SEVERITY")\
          .min("SEVERITY BY ZIP RATIO")\
          .orderBy(col("`4.EXTREME`").desc(), col("`3.MAJOR`").desc()).limit(40).show(20)


# In[ ]:
