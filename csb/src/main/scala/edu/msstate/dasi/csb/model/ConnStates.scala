package edu.msstate.dasi.csb.model

/**
 * The states of a connection.
 */
object ConnStates extends Enumeration {
  /** Connection status unknown or invalid. */
  val UNKNOWN = Value

  /** Connection attempt seen, no reply. */
  val S0 = Value

  /** Connection established, not terminated. */
  val S1 = Value

  /**
   * Normal establishment and termination. Note that this is the same symbol as for state S1. You
   * can tell the two apart because for S1 there will not be any byte counts in the summary, while
   * for SF there will be.
   */
  val SF = Value

  /** Connection attempt rejected. */
  val REJ = Value

  /** Connection established and close attempt by originator seen (but no reply from responder). */
  val S2 = Value

  /** Connection established and close attempt by responder seen (but no reply from originator). */
  val S3 = Value

  /** Connection established, originator aborted (sent a RST). */
  val RSTO = Value

  /** Responder sent a RST. */
  val RSTR = Value

  /** Originator sent a SYN followed by a RST, we never saw a SYN-ACK from the responder. */
  val RSTOS0 = Value

  /** Responder sent a SYN ACK followed by a RST, we never saw a SYN from the (purported) originator. */
  val RSTRH = Value

  /**
   * Originator sent a SYN followed by a FIN, we never saw a SYN ACK from the responder (hence the
   * connection was “half” open).
   */
  val SH = Value

  /** Responder sent a SYN ACK followed by a FIN, we never saw a SYN from the originator. */
  val SHR = Value

  /** No SYN seen, just midstream traffic (a “partial connection” that was not later closed). */
  val OTH = Value
}
