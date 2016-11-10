/**
  * Created by spencer on 11/9/16.
  */
case class edgeData(TS: String,
                    PROTOCOL: String,
                    ORIG_BYTES: Int,
                    RESP_BYTES: Int,
                    CONN_STATE: String,
                    ORIG_PKTS: Int,
                    ORIG_IP_BYTES: Int,
                    RESP_PKTS: Int,
                    RESP_IP_BYTES: Int,
                    DESC: String)

case class nodeData(data: String)
