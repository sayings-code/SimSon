import day_report.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object day_table_2 {

  val conf = new SparkConf().setAppName("dayreport").setMaster("local[10]")
  val context = new SparkContext(conf)
  val spark = SparkSession.builder().getOrCreate()
  //隐式转换
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val df_SKU_Part = Data_fetch_read("Goods-2020")

    val tablename = "产品汇总7"

    //增长top5:
    val increase = Data_fetch_read(tablename)
      .select('产品,$"毛利￥_近7天",$"数量_近7天",$"毛利￥_7天增长",$"毛利￥_7天增长率").limit(10)
    //整合表
    val increase_result = Data_join(increase,df_SKU_Part)
    val Increase_Result = increase_result
      .select('产品,$"毛利￥_近7天",$"数量_近7天",$"毛利￥_7天增长",$"毛利￥_7天增长率",'产品经理)
      .sort(desc("毛利￥_7天增长"))
    //入库
    Data_fetch_save(Increase_Result,"CP_increase.xlsx")


    //下降top5
    val describe_1 = Data_fetch_read(tablename).repartition(1)
      .withColumn("id",monotonically_increasing_id()+1)
      .sort($"id".desc)
    val skip = describe_1.first()
    val describe = describe_1.filter(row => row != skip).limit(10).select('产品,$"毛利￥_近7天",$"数量_近7天",$"毛利￥_7天增长",$"毛利￥_7天增长率")
    //整合表
    val describe_result = Data_join(describe,df_SKU_Part)
    val Describe_Result = describe_result
      .select('产品,$"毛利￥_近7天",$"数量_近7天",$"毛利￥_7天增长",$"毛利￥_7天增长率",'产品经理)
      .sort(desc("毛利￥_7天增长"))
    //入库
    Data_fetch_save(Describe_Result,"CP_describe.xlsx")


  }

  //数据落地的方法
  def  Data_fetch_save(data:DataFrame,Path:String)={
    data.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .option("dateFormat", "yyyy-mm-dd")
      .option("timestampFormat", "yyyy-mm-dd")
      .mode("overwrite")
      .save("datasource/"+Path)
    println("--数据执行成功--")
  }

  //读表方法
  def Data_fetch_read(table_name:String): DataFrame ={
    val df = spark.read.format("com.crealytics.spark.excel")
      .option("useHeader","true")
      .load("datasource/" + table_name + ".xlsx")
    df
  }

  //连接表
  def Data_join(df1:DataFrame,df2:DataFrame): DataFrame ={
    //join操作，连接表
    val join = df1
      .join(df2,'产品 === 'SKU,"left")
    join
  }
}
