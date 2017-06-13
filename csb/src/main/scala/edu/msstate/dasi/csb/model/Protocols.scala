package edu.msstate.dasi.csb.model

/**
 * The transport layer protocol of a connection.
 */
object Protocols extends Enumeration {
  /** An unknown transport-layer protocol. */
  val UNKNOWN = Value

  /** Transmission Control Protocol. */
  val TCP = Value

  /** User Datagram Protocol. */
  val UDP = Value

  /** Internet Control Message Protocol. */
  val ICMP = Value
}
