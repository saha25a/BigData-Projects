
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object BigLog {
  
  case class Logging(level:String, datetime:String)

  def mapper(line:String): Logging = {
    
   val fields = line.split('.')
   val logging:Logging = Logging(fields(0), fields(1))
   
   return logging
    }
  
  //Where all the magic happens
  def main(args: Array[String]) {
    
  //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

  //Use new SparkSession interface in spark 2.0
    val spark = SparkSession
       .builder
       .appName("SparkSQL")
       .master("local[*]")
       .getOrCreate()
    
   // import spark.implicits._
    
    val df = spark.read
      .option("header",true)
      .csv("G:/Trendytech/Spark_dataset/biglog.txt")
      
    df.createOrReplaceTempView("Log_Table")
    
    val results = spark.sql("""
      select level, date_format(datetime, 'MMMM') as month, count(1) as total
      from Log_Table group by level, month""")
      
    val columns = List("January", "February", "March", "April", "May", "June", "July",
                       "August", "September", "October", "November", "December")
      
    val result1 = spark.sql("""select level, date_format(datetime, 'MMMM') as month
     from Log_Table""").groupBy("level").pivot("month",columns).count().show(100)
           
   }
  }