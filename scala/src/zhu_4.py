from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
import numpy as np
from datetime import datetime

sc = SparkContext()
sqlcontext = SQLContext(sc)

crime_path = 'hdfs://wolf.analytics.private/user/zzm7646/data/Crimes_-_2001_to_present.csv' 

crime = sqlcontext.read.csv(crime_path, header = True)

# crime with arrest
crime4 = crime.filter(crime['Arrest']=='true')


# function to get day of the week
funcWeekDay =  udf(lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p').strftime('%w'))
funcHour =  udf(lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p').strftime('%H'))


# get month, weekday and time of the day (hour) 
crime4 = crime4.withColumn('Month', col('Date').substr(1, 2))\
               .withColumn('Hour',funcHour(col('Date')))\
               .withColumn('weekDay', funcWeekDay(col('Date')))

######################## groupby hour  ########################
time_arrest = crime4.groupBy('Hour').count().orderBy('count', ascending=False)
pddf_time_day_arrest = time_arrest.toPandas()
pddf_time_day_arrest.rename(columns={'count':'Time Of Day Crimes with Arrest'},inplace=True)

# write to txt
writePath = '/nfs/home/zzm7646/hw2/ex4/zhu_4_time.txt'   
with open(writePath, 'a') as f:
    f.write(pddf_time_day_arrest.to_string(header = False, index = False))

# plot
ax1 = pddf_time_day_arrest.sort_values(by='Hour').plot.line(x='Hour', y='Time Of Day Crimes with Arrest',grid=True)
# major_ticks1 = np.arange(00, 24, 1)
ax1.set_xticks(major_ticks1)
ax1.figure.savefig('/nfs/home/zzm7646/hw2/ex4/time_of_day_crime_arrest_line.jpg',dpi=480)

######################## groupby weekDay ########################
weekday_arrest = crime4.groupBy('weekDay').count().orderBy('count', ascending=False)
pddf_weekday_arrest = weekday_arrest.toPandas()
pddf_weekday_arrest['weekDay'] = pddf_weekday_arrest['weekDay'].apply(lambda x: int(x))
pddf_weekday_arrest.rename(columns={'count':'Week of Day Crimes with Arrest'},inplace=True)

# write to txt
writePath = '/nfs/home/zzm7646/hw2/ex4/zhu_4_wk.txt'  
with open(writePath, 'a') as f:
    f.write(pddf_weekday_arrest.to_string(header = False, index = False))

# plot
ax2 = pddf_weekday_arrest.sort_values(by='weekDay').plot.line(x='weekDay', y='Week of Day Crimes with Arrest',grid=True)
major_ticks2 = np.arange(0, 7, 1)
ax2.set_xticks(major_ticks2)
ax2.figure.savefig('/nfs/home/zzm7646/hw2/ex4/weekDay_crime_arrest_line.jpg',dpi=480)



######################## groupby month ########################
monthly_arrest = crime4.groupBy('Month').count().orderBy('count', ascending=False)
pddf_monthly_arrest = monthly_arrest.toPandas()
pddf_monthly_arrest['Month'] = pddf_monthly_arrest['Month'].apply(lambda x: int(x))
pddf_monthly_arrest.rename(columns={'count':'Monthly Crimes with Arrest'},inplace=True)

# write to txt
writePath = '/nfs/home/zzm7646/hw2/ex4/zhu_4_mo.txt'  
with open(writePath, 'a') as f:
    f.write(pddf_monthly_arrest.to_string(header = False, index = False))

# plot
ax3 = pddf_monthly_arrest.sort_values(by='Month').plot.line(x='Month', y='Monthly Crimes with Arrest',grid=True)
major_ticks3 = np.arange(1, 13, 1)
ax3.set_xticks(major_ticks3)
ax3.figure.savefig('/nfs/home/zzm7646/hw2/ex4/monthly_crime_arrest_line.jpg',dpi=480)
