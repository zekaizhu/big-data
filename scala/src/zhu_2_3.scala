object zhu_2_3 {
   def main(args:Array[String]):Unit = {

      val crime_path = "hdfs://wolf.analytics.private/user/zzm7646/data/Crimes_-_2001_to_present.csv" 
      //val crime_path = "file:/Users/zach/hw2/Crimes_-_2001_to_present.csv"



      // Mayor Daly left the office in May 2011, we take full year to avoid overlapping
      val rdd = sc.textFile(crime_path).
                        map(t=>t.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).
                        filter(lines => lines(0)!= "ID")


      val rdd_daly = rdd.filter(t=>t(17).toInt <= 2010)

      // annul # of crime events
      rdd_daly.map(t=>t(17)).
               map(t=>(t,1)).             
               reduceByKey(_+_).
               sortByKey().
               saveAsTextFile("hdfs://wolf.analytics.private/user/zzm7646/hw2/exercise2/task3/output/daly/annual/")
               //saveAsTextFile("file:/Users/zach/hw2/zhu_2_3_annual_daly.txt")


      // annul # of crime events by beat
      rdd_daly.map(t=>t(10)).
               map(t=>(t,1)).          
               reduceByKey(_+_).
               mapValues(x => x/10).
               sortBy(_._2, false).
               saveAsTextFile("hdfs://wolf.analytics.private/user/zzm7646/hw2/exercise2/task3/output/daly/beat/")
               //saveAsTextFile("file:/Users/zach/hw2/zhu_2_3_loc_daly.txt")


      // annul # of crime events by crime type
      rdd_daly.map(t=>t(4)).
               map(t=>(t,1)).          
               reduceByKey(_+_).
               mapValues(x => x/10).
               sortBy(_._2, false).
               saveAsTextFile("hdfs://wolf.analytics.private/user/zzm7646/hw2/exercise2/task3/output/daly/iucr/")
               //saveAsTextFile("file:/Users/zach/hw2/zhu_2_3_iucr_daly.txt")



      // Mayor Emanuel was in the office from May 2011 to 2019 May, we take full year to avoid overlapping
      val rdd_emanuel = rdd.filter(t=>t(17).toInt >= 2012 && t(17).toInt <= 2018)
         
       
      // annul # of crime events
      rdd_emanuel.map(t=>t(17)).
                  map(t=>(t,1)).             
                  reduceByKey(_+_).
                  sortByKey().
                  saveAsTextFile("hdfs://wolf.analytics.private/user/zzm7646/hw2/exercise2/task3/output/emanuel/annual/")
                  //saveAsTextFile("file:/Users/zach/hw2/zhu_2_3_annual_emanuel.txt")


      // annul # of crime events by beat
      rdd_emanuel.map(t=>t(10)).
                  map(t=>(t,1)).          
                  reduceByKey(_+_).
                  mapValues(x => x/7).
                  sortBy(_._2, false).
                  saveAsTextFile("hdfs://wolf.analytics.private/user/zzm7646/hw2/exercise2/task3/output/emanuel/beat/")
                  //saveAsTextFile("file:/Users/zach/hw2/zhu_2_3_loc_emanuel.txt")


      // annul # of crime events by crime type
      rdd_emanuel.map(t=>t(4)).
                  map(t=>(t,1)).          
                  reduceByKey(_+_).
                  mapValues(x => x/7).
                  sortBy(_._2, false).
                  saveAsTextFile("hdfs://wolf.analytics.private/user/zzm7646/hw2/exercise2/task3/output/emanuel/iucr")
                  //saveAsTextFile("file:/Users/zach/hw2/zhu_2_3_iucr_emanuel.txt")
   }
}      