import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import java.io._
import scala.collection.mutable.ListBuffer


object zhu_2_2 {
   def main(args:Array[String]):Unit = {
      //val crime_path = "file:/Users/zach/hw2/Crimes_-_2001_to_present.csv" 
      val crime_path = "hdfs://wolf.analytics.private/user/zzm7646/data/Crimes_-_2001_to_present.csv"

      val years = List("2015","2016","2017","2018","2019")
      val rdd = sc.textFile(crime_path).
                  map(t=>t.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).
                  filter(t=>years.contains(t(17))).
                  map(t=>((t(17),t(10)),1)).
                  reduceByKey(_+_).
                  sortByKey()

      //get beat array(String)
      val beat = rdd.keys.map(x => x._2).collect().toList

      val data = rdd.values.
                     map(_.toDouble).
                     zipWithIndex(). //Add index, and data is sorted
                     groupBy(_._2/274). // _._2 is the index which we divide by required dimension(5 here) for groups of that size
                     map(_._2.map(_._1)).  //_._2 here is the grouped contents on which we apply map to get the original rdd contents, ditch the index
                     map(t=>t.toArray).
                     map(t=>Vectors.dense(t))

      //corr matrix 
      val corrMatrix: Matrix = Statistics.corr(data, "pearson")


      // get pairwaise corr
      val pairwiseArr = new ListBuffer[Array[Double]]()

      for( i <- 0 to corrMatrix.numRows-1){
        for(j <- 0 to corrMatrix.numCols-1){
          if( i > j ){
            pairwiseArr += Array(beat(i).toDouble,beat(j).toDouble,corrMatrix.apply(i,j)) 
          }
        }
      }

      case class pairRow(beat1: Long, beat2: Long, corr: Double)    

      val pairwiseDF = pairwiseArr.map(x => pairRow(Math.round(x(0)), Math.round(x(1)), x(2))).toDF()

      //find the max corr among beats
      val df = pairwiseDF.filter(pairwiseDF("corr") < 1.0).orderBy(desc("corr"))
      df.show(50)

      //write to txt
      df.write.format("com.databricks.spark.csv").option("header", "true").save("hdfs://wolf.analytics.private/user/zzm7646/hw2/exercise2/task2/output/")
  }
}  