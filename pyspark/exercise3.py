from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StructType, StructField
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
#sc.install_pypi_package("pandas")  # when running in jupyter you need to install the package
#sc.install_pypi_package("s3fs")
import pandas as pd


if __name__ == "__main__":

############################################## Initialize ###########################################
sc = SparkContext()
sqlcontext = SQLContext(sc)
sc.setLogLevel('ERROR')

############################################## Load Data ###########################################
path = 's3://nu-zach-s3/trading.csv'
trading = sqlcontext.read.csv(path, header = True)

############################################## Clean Data ##########################################
# Create groups for bar_num
trading_clean = trading.orderBy("time_stamp",ascending=False)\
                       .withColumn('date', to_date('time_stamp','yyyy-MM-dd'))\
                       .withColumn('year',year(col('date')))\
                       .withColumn('Month', month(col('date')))\
                       .withColumn('group', ceil(col('bar_num')/10))\
                       .drop('time_stamp')\
                       .drop('direction')
                        
# Change string type to integer
trading_clean = trading_clean.select([col(c).cast("integer") for c in trading_clean.columns])

############################################## Featurize ###########################################
# Create new features
group_data = trading_clean.groupBy('trade_id','group')
group_data = group_data.agg(min(col('profit')).alias('profit_min'),
                            max(col('profit')).alias('profit_max'),
                            avg(col('profit')).alias('profit_avg'),
                            sum(col('profit')).alias('profit_sum'))

trading_clean = trading_clean.join(group_data, on = ['trade_id', 'group'])

# Subset data by trade_id and group
trading_clean_subset = trading_clean.select(col('trade_id'),
                                          col('group'),
                                          col('profit_min'),
                                          col('profit_max'),
                                          col('profit_avg'),
                                          col('profit_sum'))\
                                    .groupBy('trade_id','group',
                                             'profit_min','profit_max',
                                             'profit_avg','profit_sum').count()\
                                    .drop('count').sort('trade_id','group')

# For each group, not only var12-var78 will be used as features, 
# but the previous groups' profit(min,max,mean,sum) as additional features

# Here we are defining the schema of an empty dataframe that will grow as we traverse out loop
schema = StructType([StructField('trade_id', IntegerType(), False), 
                     StructField(('group'), IntegerType(), False), 
                     StructField(('lag_profit_min'), IntegerType(), False),
                     StructField(('lag_profit_max'), IntegerType(), False),
                     StructField(('lag_profit_avg'), IntegerType(), False),
                     StructField(('lag_profit_sum'), IntegerType(), False)])

results = sqlcontext.createDataFrame([], schema)
                     
for group in range(1,13):      
    if group == 1:
        lag_profit = trading_clean_subset.filter(col('group')==1)\
                                         .withColumn('lag_profit_min',col('profit_min'))\
                                         .withColumn('lag_profit_max',col('profit_max'))\
                                         .withColumn('lag_profit_avg',col('profit_avg'))\
                                         .withColumn('lag_profit_sum',col('profit_sum'))\
                                         .select(col('trade_id'),
                                               col('group'),
                                               col('lag_profit_min'),
                                               col('lag_profit_max'),
                                               col('lag_profit_avg'),
                                               col('lag_profit_sum'))
    else:
        previous_level = trading_clean_subset.filter(col('group')==group-1)\
                                  .select(col('trade_id'),
                                          col('group'),
                                          col('profit_min'),
                                          col('profit_max'),
                                          col('profit_avg'),
                                          col('profit_sum'))
        
        lag_profit = previous_level.withColumn('group',col('group')+1)\
                                   .select(col('trade_id'),
                                           col('group'),
                                           col('profit_min').alias('lag_profit_min'),
                                           col('profit_max').alias('lag_profit_max'),
                                           col('profit_avg').alias('lag_profit_avg'),
                                           col('profit_sum').alias('lag_profit_sum'))

    results = results.union(lag_profit)

# Join with trading_clean to create the frame for modelling
trading_model = trading_clean.join(results, on = ['trade_id','group'], how='inner')


############################################### Modelling ############################################

# List to store mape
mape_list = []

# Features
fe = ['bar_num','var12', 'var13','var14', 'var15', 'var16', 'var17', 'var18', 'var23', 'var24', 'var25',
       'var26', 'var27', 'var28', 'var34', 'var35', 'var36', 'var37', 'var38','var45', 'var46', 'var47', 
       'var48', 'var56', 'var57', 'var58', 'var67','var68', 'var78',
      'lag_profit_min','lag_profit_max','lag_profit_avg','lag_profit_sum']

# Model settting
assembler = VectorAssembler(inputCols=fe, outputCol='features').setHandleInvalid("skip")
rf = RandomForestRegressor(labelCol="profit", featuresCol="features")
pipeline = Pipeline(stages=[assembler, rf])

# Train model
for yr in range(2008,2016):
    # Train from Jan to June, or July to Dec
    for mo in (1,7):
        if (yr == 2015) & (mo==7): break
            
        train = trading_model.filter((col('year')==yr) & (col('month') >= mo) & (col('month') <= mo+5))
        test = trading_model.filter((col('year')== (yr + mo//7)) & (col('month')== (mo+6)%12))

        model_rf = pipeline.fit(train)
        test_rf = model_rf.transform(test)
        
        # Calculate mape
        mape = test_rf.select('profit','prediction')\
                      .withColumn("ape",abs((col('prediction')-col('profit'))/col('profit')))\
                      .agg(avg(col('ape'))).collect()[0][0]
                                           
        date = str(yr + mo//7) + '-' + str((mo+6)%12)
        mape_list.append([date,mape])  


############################################# Save results ##########################################
df = pd.DataFrame(mape_list, columns=['Date','MAPE'])

# Add max, min and avg for MAPE
mmm_list=[['Max MAPE:', df.MAPE.max()],['Min MAPE:',df.MAPE.min()],['Average MAPE:',df.MAPE.mean()]]
df1 = pd.DataFrame(mmm_list, columns=['Date','MAPE'])
df_final = pd.concat([df,df1],axis=0)        

# Save to s3 bucket 
df_final.to_csv('s3://nu-zach-s3/Exercise3.txt',index=False, sep=' ', mode='a')
sc.stop()  