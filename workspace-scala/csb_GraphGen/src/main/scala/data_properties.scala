/**
  * Created by spencer on 11/9/16.
  */

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
case class edgeData(TS: String,
                    PROTOCOL: String,
                    ORIG_BYTES: Long,
                    RESP_BYTES: Long,
                    CONN_STATE: String,
                    ORIG_PKTS: Long,
                    ORIG_IP_BYTES: Long,
                    RESP_PKTS: Long,
                    RESP_IP_BYTES: Long,
                    DESC: String)

/***
  *
  * @param data Concatenated string with SourceIP-DestIP:Port:Connection_Type
  */
case class nodeData(data: String)
