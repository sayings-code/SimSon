package sayings.simson.report

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import sayings.simson.read.Data_Fetch


object Personal extends Data_Fetch{
    def main(args: Array[String]): Unit = {

        //导入隐式转换
        import spark.implicits._

        // 取出总数据
        val step1: DataFrame = Data_fetch
          .filter('number >= 1)
          .select('BM, 'fk, 'ML,'PS,'cash2mub)

        //去除空值
        val step2 = step1.na.fill("0", Array[String]("a","cash2mub"))

        // 初步筛选
        val step3 = step2
          .groupBy('PS.as("负责人"))
          .agg(sum($"ML").as("达成量"))
          .sort('达成量.desc)

        //营业表
        val result3 = step2
          .groupBy('PS.as("fz"))
          .agg(sum($"cash2mub").as("营业收入"))

        // 保留两位小数
        val step4 = step3
          .select('负责人,bround($"达成量",scale = 2).alias("达成量"))

        val result4 = result3
          .select('fz,bround($"营业收入",scale = 2).alias("营业收入"))

        //数据库取出负责人任务量表
        val df: DataFrame = Mysql_Data_fetch_ps
        val df1 = df.toDF("fz","任务量")

        // 合并任务量表
        val step5 = step4.join(df1,'负责人 === 'fz,"inner")

        //计算达成率
        val step6 = step5.withColumn("达成率",'达成量 / '任务量 * 100)

        //添加达成排名
        val step7 = step6
          .withColumn("达成排名",dense_rank
            .over(Window.orderBy(desc("达成量"))))

        //达成率保留两位小数
        val step8 = step7
          .select('负责人,'任务量,'达成量,round($"达成率",scale = 2)
            .alias("达成率"),'达成排名)

        //添加达成率排名
        val step9 = step8
          .withColumn("达成率排名",dense_rank()
            .over(Window.orderBy(desc("达成率"))))

        // 合并营业收入表
        val step10 = step9.join(result4,'负责人 === 'fz,"inner")

        //达成率填充
        val step11 = step10.na.fill(100)

        //计算毛利率
        val step12 = step11
          .withColumn("毛利率",'达成量 / '营业收入 * 100)

        //筛选最终结果
        val step13 = step12
          .select('负责人,'任务量,'达成量,$"达成率",'达成排名,'达成率排名,'营业收入,
              round($"毛利率",scale = 2).alias("毛利率")).sort("达成排名")


        //构建总计那一行的数据
        val Total_head:DataFrame = Seq(total(1,"总计")).toDF()

        val step17 = step13.groupBy()
          .agg(sum("达成量"),
              sum("任务量"),
              min("达成排名"),
              sum("营业收入")).toDF("达成量","任务量","达成排名","营业收入")
        val step18 = step17.withColumn("达成率",'达成量 / '任务量 * 100)
          .withColumn("毛利率",'达成量 / '营业收入 * 100)
          .withColumn("达成率排名",'达成排名 * 0)

        val step19 = step18.join(Total_head,'达成排名 === 'id,"inner")

        val step20 = step19.select('负责人,
            bround($"任务量",scale = 1).alias("任务量"),
            bround($"达成量",scale = 1).alias("达成量"),
            bround($"达成率",scale = 1).alias("达成率"),
            '达成排名,'达成率排名,
            bround($"营业收入",scale = 1).alias("营业收入"),
            bround($"毛利率",scale = 1).alias("毛利率"))

        //联合总计行数据
        val result1 = step13.union(step20)


        //拼接  % 上去
        val result = result1.select('负责人,'任务量,'达成量,
            concat($"达成率",lit("%")).alias("达成率"),
            '达成排名,'达成率排名,'营业收入,
            concat($"毛利率",lit("%")).alias("毛利率"))

        result.show(35)
        //    数据落地到xlsx中
        def task_ps(textfilename:String): Unit = {
            result.write
              .format("com.crealytics.spark.excel")
              .option("dataAddress", "'个人达成'!A1")
              .option("useHeader", "true")
              .mode("append")
              .save("dataset/" + textfilename + ".xlsx")
        }
        task_ps("2020-10-28总数据报表")
    }
    case class total(id: Int, 负责人: String)
}
