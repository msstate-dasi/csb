package edu.msstate.dasi.csb.model

/**
 *
 */
case class VertexData()

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
}
