package edu.msstate.dasi.csb.model

object Protocols extends Enumeration {
  /**
    * transport_proto  Meaning
    * UNKNOWN  An unknown transport-layer protocol.
    * TCP  Transmission Control Protocol
    * UDP  User Datagram Protocol
    * ICMP  Internet Control Message Protocol
    *
    */
  val UNKNOWN, TCP, UDP, ICMP = Value
}
