package sayings.simson.report

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import sayings.simson.read.Data_Fetch

object PlatForm extends Data_Fetch {

    def main(args: Array[String]): Unit = {
        //导入隐式转换
        import spark.implicits._

        //取出总数据
        val step1: DataFrame = Data_fetch
          .filter('number>=1)
          .select('BM,'fk,'ML,'cash2mub)

        //去除空值
        val step2: DataFrame = step1.na.fill("0",Array[String]("ML","cash2mub")).toDF()

        //初步筛选
        val step3: Dataset[Row] = step2
          .groupBy('fk.alias("平台"))
          .agg(sum($"ML").as("达成量"))
          .sort('达成量.desc)

        //筛选营业收入
        val result3 = step2
          .groupBy('fk)
          .agg(sum($"cash2mub").as("营业收入"))

        //保留两位小数
        val result4 = result3
          .select('fk,bround($"营业收入",scale = 2)
            .alias("营业收入"))

        val step4 = step3
          .select('平台,bround($"达成量",scale = 2)
            .alias("达成量"))


        //数据库中取出任务量表
        val df: DataFrame = Mysql_Data_fetch_pt
        val df1 = df.toDF("pt","任务量")

        //合并两表
        val step6 = step4.join(df1,'平台 === 'pt,"inner")
        val step7 = step6.select('平台,'任务量,'达成量)

        // 计算相应指标
        val step8 = step7.withColumn("达成率",'达成量 / '任务量 * 100 + 0)

        //保留两位小数
        val step9  = step8
          .select('平台,'任务量,'达成量,round($"达成率",scale = 2)
            .alias("达成率"))

        //达成排名
        val step11 = step9
          .withColumn("达成排名",dense_rank
            .over(Window.orderBy(desc("达成量"))))

        //达成率排名
        val step12 = step11
          .withColumn("达成率排名",dense_rank
            .over(Window.orderBy(desc("达成率"))))

        //合并营业收入表
        val step13 = step12.join(result4,'平台 === 'fk,"inner")
        val step14 = step13
          .select("平台","任务量","达成量","达成率","达成排名","达成率排名","营业收入")

        //计算指标毛利率
        val step15 = step14
          .withColumn("毛利率",'达成量 / '营业收入 * 100)
          .sort('达成排名)

        //毛利率保留两位小数
        val step16 = step15
          .select('平台,'任务量,'达成量,$"达成率", '达成排名,'达成率排名,'营业收入,
              round($"毛利率",scale = 2).alias("毛利率"))
        //构建总计那一行的数据
        val Total_head:DataFrame = Seq(total(1,"总计")).toDF()

        val step17 = step16.groupBy()
          .agg(sum("达成量"),
              sum("任务量"),
              min("达成排名"),
              sum("营业收入")).toDF("达成量","任务量","达成排名","营业收入")
        val step18 = step17.withColumn("达成率",'达成量 / '任务量 * 100)
          .withColumn("毛利率",'达成量 / '营业收入 * 100)
          .withColumn("达成率排名",'达成排名 * 0)

        val step19 = step18.join(Total_head,'达成排名 === 'id,"inner")

        val step20 = step19.select('平台,
            bround($"任务量",scale = 1).alias("任务量"),
            bround($"达成量",scale = 1).alias("达成量"),
            bround($"达成率",scale = 1).alias("达成率"),
            '达成排名,'达成率排名,
            bround($"营业收入",scale = 1).alias("营业收入"),
            bround($"毛利率",scale = 1).alias("毛利率")
        )
        //联合总计行数据
        val result1 = step16.union(step20)

        //拼接  % 上去
        val result = result1.select('平台,'任务量,'达成量,
            concat($"达成率",lit("%")).alias("达成率"),
             '达成排名,'达成率排名,'营业收入,
            concat($"毛利率",lit("%")).alias("毛利率"))

        result.show()

        //    数据落地到xlsx中
        def task_pt(textfilename:String)={
            result.write
              .format("com.crealytics.spark.excel")
              .option("dataAddress","'平台达成'!A1")
              .option("useHeader","true")
              .mode("append")
              .save("dataset/"+textfilename+".xlsx")
        }

        task_pt("2020-10-28总数据报表")
    }

    case class total(id:Int,平台:String)
}
