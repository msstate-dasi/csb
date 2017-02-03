package edu.msstate.dasi.csb

/**
 *
 *
 * @param proto Level 3 Protocol
 * @param duration
 * @param origBytes Original Bytes sent from source to destination
 * @param respBytes Response Bytes sent from destination to source
 * @param connState Connection State at termination of the connection
 * @param origPkts Original Packets sent from source to destination
 * @param origIpBytes Original IP Packet Bytes sent from source to destination
 * @param respPkts  Response Packets sent from destination to source
 * @param respIpBytes  Response IP Bytes sent from destination to source
 * @param desc Connection description
 */
case class EdgeData(/* ts: Date, */
                    /* uid: String */
                    proto: String,
                    /* service: String, */
                    duration: Double,
                    origBytes: Long,
                    respBytes: Long,
                    connState: String,
                    /* localOrig: Boolean, */
                    /* localResp: Boolean, */
                    /* missedBytes: Long, */
                    /* history: String, */
                    origPkts: Long,
                    origIpBytes: Long,
                    respPkts: Long,
                    respIpBytes: Long,
                    /* tunnelParents: String, */
                    desc: String)
