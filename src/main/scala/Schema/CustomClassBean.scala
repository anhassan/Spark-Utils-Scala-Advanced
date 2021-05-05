package Schema

import scala.beans.BeanProperty

@SerialVersionUID(100L)
class CustomClassBean(@BeanProperty var field1: Int,
                      @BeanProperty var field2: Int,
                      @BeanProperty var field3: String) extends Serializable {
  override def toString = s"CustomClassBean: $field1 $field2 $field3"
}