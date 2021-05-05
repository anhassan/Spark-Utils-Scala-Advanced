package BiggerCaseClasses

import Schema.CustomClassBean
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object BeanToDS extends App {
  val sparkSession = SparkSession.builder()
    .appName("BiggerCustomClass")
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._

  implicit val beanEncoder: Encoder[CustomClassBean] = Encoders.bean[CustomClassBean](classOf[CustomClassBean])
  val customClassBean = new CustomClassBean(1,2,"A")

  val customDS = sparkSession.sparkContext.parallelize(Seq(customClassBean)).toDS()
  customDS.show(false)
  /*
  +------+------+------+
  |field1|field2|field3|
  +------+------+------+
  |1     |2     |A     |
  +------+------+------+
   */

  val customDF = Seq((1,2,"B")).toDF("field1","field2","field3")
  val customDFDS = customDF.as[CustomClassBean](beanEncoder)

  customDFDS.show(false)
  /*
  +------+------+------+
  |field1|field2|field3|
  +------+------+------+
  |1     |2     |B     |
  +------+------+------+

   */
  
}
