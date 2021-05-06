package BiggerCaseClasses

import Schema.Structure
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.annotation.tailrec

object NestedCaseClassToDS extends App with SparkSessionTrait {
  import sparkSession.implicits._

  val nestedCaseClassDF = Seq(("A","B",1,2,3)).toDF("field1","field2","subfield1","subfield2","subfield3")
  nestedCaseClassDF.show(false)
  /*
  +------+------+---------+---------+---------+
  |field1|field2|subfield1|subfield2|subfield3|
  +------+------+---------+---------+---------+
  |A     |B     |1        |2        |3        |
  +------+------+---------+---------+---------+
   */

  // Get the Schema of the Nested Case Class
  val schema = ScalaReflection.schemaFor[Structure].dataType.asInstanceOf[StructType].toList
  // Convert DF(unflattened) into DS(nested)
  val nestedCaseClassDS = nestedCaseClassDF.select(schemaToColumns(schema):_*).as[Structure]

  nestedCaseClassDS.show(false)
  /*
  +------+------+------------+
  |field1|field2|subStructure|
  +------+------+------------+
  |A     |B     |[1, 2, 3]   |
  +------+------+------------+
   */

  /**
    * Utility function to convert schema of nested case classes to columns
    * for conversion of DF to type-safe DS
     */

  def schemaToColumns(schema:List[StructField]):List[Column] = schema.map(field=>
  field.dataType match {
    case st:StructType => struct(cols = schemaToColumns(st.fields.toList):_*).as(field.name)
    case _ => col(field.name)
  })

}
