package BiggerCaseClasses

import Schema.CustomClassBean
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object BeanToDS extends App {

  // Creating a spark-session(entry point into the spark application)
  val sparkSession = SparkSession.builder()
    .appName("BiggerCustomClass")
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._

  // Encoder for conversion of scala bean to DS
  implicit val beanEncoder: Encoder[CustomClassBean] = Encoders.bean[CustomClassBean](classOf[CustomClassBean])
  // Intiallizing a scala bean
  val customClassBean = new CustomClassBean(1,2,"A")
  // Get schema of the bean class
  val schemaBean = ExpressionEncoder.javaBean(classOf[CustomClassBean]).schema
  // Converting a scala bean to DS
  val customDS = sparkSession.sparkContext.parallelize(Seq(customClassBean)).toDS()

  customDS.show(false)
  /* Output of Scala bean being converted to DS
  +------+------+------+
  |field1|field2|field3|
  +------+------+------+
  |1     |2     |A     |
  +------+------+------+
   */

  // Intiallizing a DF
  val customDF = Seq((1,2,"B")).toDF("field1","field2","field3")
  // Converting DF to DS via Bean Encoder
  val customDFDS = customDF.as[CustomClassBean](beanEncoder)

  customDFDS.show(false)
  /* Output of Conversion of DF to DS via Bean Encoder
  +------+------+------+
  |field1|field2|field3|
  +------+------+------+
  |1     |2     |B     |
  +------+------+------+

   */
  
}
