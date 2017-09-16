package kmeans

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.IntegerType

object UberAnalysis {
  
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val conf = new SparkConf().setMaster("local").setAppName("UberAnalysis")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    
    
    
    val uberSchema = StructType(Array(StructField("date",StringType,true),
                                      StructField("lat",DoubleType,true),
                                      StructField("long",DoubleType,true),
                                      StructField("base",StringType,true)))
                                      
    val uberFile = sc.textFile("/home/cloudera/copied from windows/KmeansUber/data/uberApr14.txt")
    val ubereFileHeader = uberFile.first()
    
    val uberRecords = uberFile.filter { x=> !x.equalsIgnoreCase(ubereFileHeader)}
    val uberRecordsWithSplit = uberRecords.map(x=> x.split(","))
    val uberRecordsRow = uberRecordsWithSplit.map{x=> 
      Row(convertToTimeStamp(x(0).replaceAll("\"","")), x(1).replaceAll("\"","").toDouble,x(2).replaceAll("\"","").toDouble,x(3).replaceAll("\"","").toString())
    }
    
    val  uberWithSchmea = hc.createDataFrame(uberRecordsRow, uberSchema)
    
    uberWithSchmea.cache()
    
    val featuresCol = Array("lat","long")
    val vectorAssemblr = new VectorAssembler().setInputCols(featuresCol).setOutputCol("features")
    val uberTransform = vectorAssemblr.transform(uberWithSchmea)
    uberTransform.cache()
    uberWithSchmea.unpersist()
    
    val Array(trainingData, testData) = uberTransform.randomSplit(Array(0.7,0.3),5043)
    trainingData.cache()
    
    
    val kmeans = new KMeans
    val km = kmeans.setK(20).setFeaturesCol("features").setPredictionCol("predection")
    val model = km.fit(trainingData)
    model.clusterCenters.foreach(println)
    
    val categories = model.transform(testData)
    
    categories.cache()
    
    categories.head(10)
    
    categories.write.format("com.databricks.spark.csv").save("uberKmeansResult.csv")
    
        
  }

  
  def convertToTimeStamp(str : String ) : String = {
    
    val format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    if(str.toString().equals(" "))
        return null
    else{
      val date = format.parse(str)
      val formatedDate = new Timestamp(date.getTime())
      return formatedDate.toString()
      
    }
  }
  
}