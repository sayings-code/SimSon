package sayings.simson.sink

import org.apache.spark._
import sayings.simson.read.Data_Fetch
import org.apache.spark.sql._
import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.sql.functions._
object Data_sink{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Data_sink").setMaster("local[6]")
    val context = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

  //创建三个任务量表:
    val Task_BM: DataFrame = Seq(
      task_BM(20201105,"德法地区部",570000),
      task_BM(20201105,"速卖通一部",215000),
      task_BM(20201105,"速卖通二部",72000),
      task_BM(20201105,"速卖通三部",218000),
      task_BM(20201105,"速卖通四部",15000),
      task_BM(20201105,"Amazon 运营部",122000),
      task_BM(20201105,"shopee 一部",285000),
      task_BM(20201105,"Lazada平台部",115000),
      task_BM(20201105,"速卖通有品部",60000),
      task_BM(20201105,"韩国地区部",75000)
    ).toDF()

    val Task_PT: DataFrame = Seq(
      task_PT(20201105,"AliExpress",555835),
      task_PT(20201105,"Amazon",122000),
      task_PT(20201105,"Auction",22500),
      task_PT(20201105,"11Street",0),
      task_PT(20201105,"Coupang",30000),
      task_PT(20201105,"Daraz",8000),
      task_PT(20201105,"Falabella",3833),
      task_PT(20201105,"Fnac",60000),
      task_PT(20201105,"Gmarket",22500),
      task_PT(20201105,"Jollychic",16000),
      task_PT(20201105,"JOOM",16667),
      task_PT(20201105,"Jumia",8000),
      task_PT(20201105,"KASPI",7500),
      task_PT(20201105,"lazada",67000),
      task_PT(20201105,"Linio",15332),
      task_PT(20201105,"Mercadolibre",3833),
      task_PT(20201105,"Newegg",16000),
      task_PT(20201105,"Rakuten.fr",60000),
      task_PT(20201105,"Real",450000),
      task_PT(20201105,"Shopee",258167),
      task_PT(20201105,"Worten",3833)
    ).toDF()

    val Task_PS: DataFrame = Seq(
      task_PS(20201105,"黄海琪",225000),
      task_PS(20201105,"林菁",225000),
      task_PS(20201105,"周绮君",120000),
      task_PS(20201105,"李嘉怡",168000),
      task_PS(20201105,"吴璇",130000),
      task_PS(20201105,"余柏财",55000),
      task_PS(20201105,"袁秋裕",45000),
      task_PS(20201105,"罗梦霞",80000),
      task_PS(20201105,"李杰玮",50000),
      task_PS(20201105,"钱美欣",78000),
      task_PS(20201105,"林嘉丽",30000),
      task_PS(20201105,"李钊婷",60000),
      task_PS(20201105,"王喜萍",146000),
      task_PS(20201105,"陈镘淇",15000),
      task_PS(20201105,"戚路彤",72000),
      task_PS(20201105,"罗汝乐",46000),
      task_PS(20201105,"李华武",0),
      task_PS(20201105,"张涣贞",25000),
      task_PS(20201105,"陈海玲",15000),
      task_PS(20201105,"黎林英",15000),
      task_PS(20201105,"黄烨丹",10000),
      task_PS(20201105,"伊万",15000),
      task_PS(20201105,"伍艳霞",0),
      task_PS(20201105,"亚马逊团队",122000)
    ).toDF()

  // 落地数据到mysql
  def saveMysql(Tablename:String,dataFrame: DataFrame): Unit ={
    val url="jdbc:mysql://localhost:3306/test"
    val table=Tablename
    val properties=new Properties()
    properties.put("user","root")
    properties.put("password","root")
    dataFrame.write.mode(SaveMode.Append).jdbc(url,table,properties)
  }
    //执行落地操作
    saveMysql("task_BM",Task_BM)
    saveMysql("task_PT",Task_PT)
    saveMysql("task_PS",Task_PS)
  }
  case class task_BM(时间:Long,部门:String,任务量:Double)
  case class task_PT(时间:Long,平台:String,任务量:Double)
  case class task_PS(时间:Long,负责人:String,任务量:Double)
}
