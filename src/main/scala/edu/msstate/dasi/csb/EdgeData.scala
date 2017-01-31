package edu.msstate.dasi.csb

/***
  *
  * @param TS             Timestamp
  * @param PROTOCOL       Level 3 Protocol, UDP/TCP
  * @param ORIG_BYTES     Original Bytes sent from source to destination
  * @param RESP_BYTES     Response Bytes sent from destination to source
  * @param CONN_STATE     Connection State at termination of the connection
  * @param ORIG_PKTS      Original Packets sent from source to destination
  * @param ORIG_IP_BYTES  Original IP Packet Bytes sent from source to destination
  * @param RESP_PKTS      Response Packets sent from destination to source
  * @param RESP_IP_BYTES  Response IP Bytes sent from destination to source
  * @param DESC           Connection description
  */
case class EdgeData(TS: String = "",
                    PROTOCOL: String = "",
                    DURATION: Double = 0,
                    ORIG_BYTES: Long = 0,
                    RESP_BYTES: Long = 0,
                    CONN_STATE: String = "",
                    ORIG_PKTS: Long = 0,
                    ORIG_IP_BYTES: Long = 0,
                    RESP_PKTS: Long = 0,
                    RESP_IP_BYTES: Long = 0,
                    DESC: String = "")
