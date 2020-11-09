package sayings.simson.report

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import sayings.simson.read.Data_Fetch
import org.apache.spark.sql.functions._

object Department extends Data_Fetch{

    def main(args: Array[String]): Unit = {
        //导入隐式转换
        import spark.implicits._

        //取出表总数据初步筛选
        val step1 = Data_fetch
          .filter('number >= 1)
          .select('BM.as("部门"), 'ML)
          .groupBy('部门)
          .agg(sum($"ML").as("达成量"))
          .sort('达成量.desc)

        //达成量保留两位小数
        val step2 = step1
          .where('达成量 >= 0)
          .select('部门, bround($"达成量", scale = 2)
            .alias("达成量"))

        //  //数据库中取出部门任务量表
        val df: DataFrame = Mysql_Data_fetch_bm

        val df1: DataFrame = df.toDF("BM", "任务量")

        //连接两表
        val step4 = step2.join(df1, '部门 === 'BM, "inner")

        val step5 = step4.select('部门, '达成量, '任务量)

        //计算指标
        val step6 = step5
          .withColumn("达成率(%)", '达成量 / '任务量 * 100)

        // 保留两位小数
        val step7 = step6
          .select('部门, '达成量, '任务量, bround($"达成率(%)", scale = 2)
            .alias("达成率(%)"))

        //达成排名
        val step8 = step7
          .withColumn("达成排名", dense_rank()
            .over(Window.orderBy(desc("达成量"))))

        //达成率排名
        val step9 = step8
          .withColumn("达成率排名", dense_rank()
            .over(Window.orderBy(desc("达成率(%)"))))

        val step10 = step9.sort("达成排名")

        //构建总计那一行的数据
        val Total_head:DataFrame = Seq(total(1,"总计")).toDF()

        val step11 = step10.groupBy()
          .agg(sum("达成量"),
              sum("任务量"),
              min("达成排名")).toDF("达成量","任务量","达成排名")
        val step12 = step11.withColumn("达成率",'达成量 / '任务量 * 100)
          .withColumn("达成率排名",'达成排名 * 0)

        val step13 = step12.join(Total_head,'达成排名 === 'id,"inner")

        val step14 = step13.select('部门,
            bround($"达成量",scale = 1).alias("达成量"),
            bround($"任务量",scale = 1).alias("任务量"),
            bround($"达成率",scale = 1).alias("达成率"),
            '达成排名,'达成率排名)

        //联合总计行数据
        val result1 = step10.union(step14)

        //拼接  % 上去
        val result = result1.select('部门,'达成量,'任务量,
            concat($"达成率(%)",lit("%")).alias("达成率"),
            '达成排名,'达成率排名)
        result.show()


        //    数据落地到xlsx中
        def task_bm(textfilename:String): Unit ={
            result.write
              .format("com.crealytics.spark.excel")
              .option("dataAddress", "'部门达成'!A1")
              .option("useHeader", "true")
              .mode("append")
              .save("dataset/"+textfilename+".xlsx")
        }

        task_bm("2020-10-28总数据报表")

    }

    case class total(id:Int,部门:String)
}

