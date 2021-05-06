package BiggerCaseClasses

import org.apache.spark.sql.SparkSession

trait SparkSessionTrait {
  val sparkSession = SparkSession.builder()
    .appName("GenericSparkSession")
    .master("local[2]")
    .getOrCreate()
}
