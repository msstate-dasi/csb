import org.spark_project.guava.primitives.UnsignedInteger

/**
  * Created by spencer on 11/9/16.
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

case class nodeData(data: String)
