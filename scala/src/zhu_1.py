from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *

sc = SparkContext()
sc.setLogLevel('ERROR')
sqlcontext = SQLContext(sc)

crime_path ='hdfs://wolf.analytics.private/user/zzm7646/data/Crimes_-_2001_to_present.csv'  
#crime_path = 'file:/Users/zach/hw2/Crimes_-_2001_to_present.csv'

crime = sqlcontext.read.csv(crime_path, header = True)

# get month from date
new_crime = crime.withColumn('Month', col('Date').substr(1, 2))

# # group by month and year get monthly crime events
# monthly_crime = new_crime.groupBy('Year','Month').count()

# # group by month get average monthly crime events
# avg_monthly_crime = monthly_crime.groupBy('Month').mean('count').orderBy('avg(count)', ascending=False)
# round decimal and plus 1 for average monthly crime events
# pddf_crime.iloc[:,1] = pddf_crime.iloc[:,1].apply(lambda x: int(x)+1)
# pddf_crime.rename(columns={'avg(count)':'Average Monthly Crime Events'},inplace=True)


#####################SQL######################
# Register the table
new_crime.createOrReplaceTempView('t1')
avg_monthly_crime = sqlcontext.sql("""SELECT  Month, AVG(monthly_crime) AS Average_Monthly_Crime_Events
	                                  FROM (SELECT Year, Month, COUNT(*) AS monthly_crime
                                              FROM t1
                                              GROUP BY Year, Month)
                                      GROUP BY Month
                                      ORDER BY Month ASC""")


# transform to pandas dataframe for ploting
pddf_crime = avg_monthly_crime.toPandas()

# save as txt file
writePath = '/nfs/home/zzm7646/hw2/ex1/zhu_1.txt'   
#writePath = '/Users/zach/hw2/zhu_1.txt'  

with open(writePath, 'a') as f:
    f.write(pddf_crime.to_string(header = False, index = False))

# save the histgram as jpg file
ax = pddf_crime.plot.bar(x='Month', y='Average_Monthly_Crime_Events',figsize=(10,8))
ax.figure.savefig('/nfs/home/zzm7646/hw2/ex1/average_monthly_crime_events.jpg')
#ax.figure.savefig('/Users/zach/hw2/average_monthly_crime_events.jpg')