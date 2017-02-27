package edu.msstate.dasi.csb

/**
 *
 */
case class VertexData() {
  def toMap: Map[String, Any] = Map.empty
}

object VertexData {
  /**
   *
   */
  def apply(text: String): VertexData = {
    if (text == "null") {
      null.asInstanceOf[VertexData]
    } else {
      new VertexData()
    }
  }

  def toNullMap: Map[String, Any] = Map.empty

  def neo4jTemplate(prefix: String): String = ""
}
