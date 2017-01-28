package edu.msstate.dasi

import org.scalatest.FunSuite

/**
  * Created by B1nary on 1/27/2017.
  */
class KroFitTest extends FunSuite {

  test("testGetEdgeLL") {
    val obj = new KroFit(null, 0, "", 0, "")
    assert(obj.getEdgeLL(30L,31L)===4.87777)
  }

}
