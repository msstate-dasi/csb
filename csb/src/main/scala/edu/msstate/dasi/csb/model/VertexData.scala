package edu.msstate.dasi.csb.model

/**
 * Defines the vertex data which represents the host properties.
 */
case class VertexData()

/**
 * Factory for [[VertexData]] instances.
 */
object VertexData {
  /**
   * Builds an instance from a [[String]].
   */
  def apply(text: String): VertexData = {
    if (text == "null") {
      null.asInstanceOf[VertexData]
    } else {
      new VertexData()
    }
  }
}
