from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType

#original dataset mixed with new dataset up to date
crime_path1 =  "Crimes_-_2001_to_present.csv"  
crime_path2 =  "Crimes_2020.csv"               

# crime_path1 =  "hdfs://wolf.analytics.private/user/zzm7646/data/Crimes_-_2001_to_present.csv"
# crime_path2 =  "hdfs://wolf.analytics.private/user/zzm7646/data/Crimes_2020.csv" 


sc = SparkContext()
sqlcontext = SQLContext(sc)

crime1 = sqlcontext.read.csv(crime_path1, header = True)
crime2 = sqlcontext.read.csv(crime_path2, header = True)
crime3 = crime1.union(crime2)

# get week number of year
crime4 = crime3.withColumn('short_date', col('Date').substr(1, 10))\
               .withColumn('date_time', to_timestamp('Date', 'MM/dd/yyyy'))\
               .where(col('date_time').isNotNull())\
               .withColumn('weekofYear', weekofyear('date_time'))\
               .select('Beat','Year','weekofYear','IUCR')
                


IUCR_violent = [
"110",
"130",
"261",
"262",
"263",
"264",
"265",
"266",
"271",
"272",
"273",
"274",
"275",
"281",
"291",
"312",
"313",
"031A",
"031B",
"320",
"325",
"326",
"330",
"331",
"334",
"337",
"033A",
"033B",
"340",
"041A",
"041B",
"420",
"430",
"450",
"451",
"452",
"453",
"461",
"462",
"479",
"480",
"481",
"482",
"483",
"485",
"487",
"488",
"489",
"490",
"491",
"492",
"493",
"495",
"496",
"497",
"498",
"510",
"051A",
"051B",
"520",
"530",
"550",
"551",
"552",
"553",
"555",
"556",
"557",
"558",
"610",
"620",
"630",
"650",
"810",
"820",
"850",
"860",
"865",
"870",
"880",
"890",
"895",
"910",
"915",
"917",
"918",
"920",
"925",
"927",
"928",
"930",
"935",
"937",
"938",
"1010",
"1020",
"1025",
"1090",
"1753",
"1754",
]

#### group by beat and weekofDay get weekly crime events #####

#violent crime
crime = crime4.where(crime4['IUCR'].isin(IUCR_violent)).groupBy('Beat','Year','weekofYear').count().orderBy('Beat','Year','weekofYear')

#all crime
#crime = crime4.groupBy('Beat','Year','weekofYear').count().orderBy('Beat','Year','weekofYear')

#add index
crime_new = crime.withColumn('row_num', row_number().over(Window.partitionBy('Beat').orderBy('Year')))

# Set the window
w = Window.partitionBy('Beat').orderBy(('row_num'))

# # Create the lagged weekofYear
# lag_1wk = lag('weekofYear',count=1).over(w)
# lag_2wk = lag('weekofYear',count=2).over(w)
# lag_4wk = lag('weekofYear',count=4).over(w)
# lag_24wk = lag('weekofYear',count=24).over(w)
# lag_52wk = lag('weekofYear',count=52).over(w)

# # Add the lagged values to a new column
# crime_modelling = crime_new.withColumn('1wk_lag', lag_1wk)\
#                            .withColumn('2wk_lag', lag_2wk)\
#                            .withColumn('4wk_lag', lag_4wk)\
#                            .withColumn('24wk_lag', lag_24wk)\
#                            .withColumn('52wk_lag', lag_52wk)

# Create the lagged value 
# for example: llag_1wk as number of crime lag one week back 
# ATTENTION: sometimes, lag one value may not be exactly one week, we treat it as one week back
lag_1wk = lag('count',count=1).over(w)
lag_2wk = lag('count',count=2).over(w)
lag_4wk = lag('count',count=4).over(w)
lag_24wk = lag('count',count=24).over(w)
lag_52wk = lag('count',count=52).over(w)

# Add the lagged values to a new column
crime_new2 = crime_new.withColumn('1wk_lag', lag_1wk)\
                           .withColumn('2wk_lag', lag_2wk)\
                           .withColumn('4wk_lag', lag_4wk)\
                           .withColumn('24wk_lag', lag_24wk)\
                           .withColumn('52wk_lag', lag_52wk)

# cast Year and Beat to integer
data = crime_new2.drop('row_num')\
                .withColumnRenamed('count','label')\
                .withColumn('Beat', crime_new2['Beat'].cast(IntegerType()))\
                .withColumn('Year', crime_new2['Year'].cast(IntegerType()))
#data.show(5)


# assembler for features
assembler = VectorAssembler(inputCols=['Beat','Year','weekofYear','1wk_lag','2wk_lag','4wk_lag','24wk_lag','52wk_lag'], 
                            outputCol='features').setHandleInvalid("skip")

 
# Split the data into training and test sets (20% held out for testing)
(trainingData, testData) = data.randomSplit([0.8, 0.2])

# Train a RandomForest model.
rf = RandomForestRegressor(labelCol='label', featuresCol='features')

# Chain indexer and forest in a Pipeline
pipeline = Pipeline(stages=[assembler, rf])

# fit in model
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.show(5)

# evaluation
evaluator = RegressionEvaluator(labelCol='label', predictionCol='prediction', metricName='rmse')
rmse = evaluator.evaluate(predictions)
print('Model Performance RMSE: %f' % rmse)

# output model performance
text_file = open('zhu_3_violent_crime.txt', 'w')
text_file.write('Model Performance RMSE for predicting the weekly number of violent crime events at beat level : %f' % rmse)
text_file.close()

# text_file = open('zhu_3_all_crime.txt', 'w')
# text_file.write('Model Performance RMSE for predicting the weekly number of all crime events at beat level : %f' % rmse)
# text_file.close()



