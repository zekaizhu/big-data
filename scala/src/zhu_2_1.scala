val crime_path = "hdfs://wolf.analytics.private/user/zzm7646/data/Crimes_-_2001_to_present.csv" 
val years = List("2017","2018","2019")
val rdd=sc.textFile(crime_path).
            map(t=>t.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).
            filter(t=>years.contains(t(17))).
            map(t=>t(3).split(" ")(0)).
            map(t=>(t,1)).
            reduceByKey(_+_).
            sortBy(_._2, false).
            take(10)
           
//sc.parallelize(rdd).saveAsTextFile("file:/Users/zach/hw2/zhu_2_1.txt")
sc.parallelize(rdd).saveAsTextFile("hdfs://wolf.analytics.private/user/zzm7646/hw2/exercise2/task1/output/")


