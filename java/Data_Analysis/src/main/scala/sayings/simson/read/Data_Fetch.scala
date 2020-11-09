package sayings.simson.read

import java.util.Properties

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
trait Data_Fetch {

  val conf = new SparkConf().setAppName("test").setMaster("local[6]")
  val context = new SparkContext(conf)
  val spark = SparkSession.builder().getOrCreate()
  spark.conf.set("spark.driver.allowMultipleContexts",true)
  import spark.implicits._
  //读取总数据表

  def Data_fetch:DataFrame = {
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    val frame = spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "false")
      .load("dataset/总数据2020-11-5.xlsx")
    val Data_fetch: DataFrame = frame.toDF("DDtime","FHtime","DP","DWG","shop",
      "shop1","number","case","cash1","bt","btmub","cash2","cash2DWG","cash2mub",
      "CB","cash3","CBmub","albb","trans","transform","cashCB","ML","FC","SPer","WL","HK","fk","BM","PS")
    val data_fetch = Data_fetch.na.drop(Array("ML"))
    return data_fetch
  }


  // 读取mysql中的任务量表
  def Mysql_Data_fetch_bm:DataFrame = {
    val url="jdbc:mysql://localhost:3306/test"
    val table="task_bm"
    val properties=new Properties()
    properties.put("user","root")
    properties.put("password","root")
    val df = spark.read.jdbc(url,table,properties)
    val df1: DataFrame = df.toDF("Time","部门","任务量")
    val df2 = df1.filter('Time === 20201105).select("部门","任务量")
    return df2
  }

  def Mysql_Data_fetch_pt:DataFrame = {
    val url="jdbc:mysql://localhost:3306/test"
    val table="task_pt"
    val properties=new Properties()
    properties.put("user","root")
    properties.put("password","root")
    val df = spark.read.jdbc(url,table,properties)
    val df1: DataFrame = df.toDF("Time","平台","任务量")
    val df2 = df1.filter('Time === 20201105).select("平台","任务量")
    return df2
  }

  def Mysql_Data_fetch_ps:DataFrame = {
    val url="jdbc:mysql://localhost:3306/test"
    val table="task_ps"
    val properties=new Properties()
    properties.put("user","root")
    properties.put("password","root")
    val df = spark.read.jdbc(url,table,properties)
    val df1: DataFrame = df.toDF("Time","负责人","任务量")
    val df2 = df1.filter('Time === 20201105).select("负责人","任务量")
    return df2
  }
}
