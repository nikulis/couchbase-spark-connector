package com.couchbase.spark.streaming

import com.couchbase.client.core.ClusterFacade
import com.couchbase.client.core.config.CouchbaseBucketConfig
import com.couchbase.client.core.message.cluster.{GetClusterConfigRequest, GetClusterConfigResponse}
import com.couchbase.client.core.message.dcp.{StreamRequestRequest, StreamRequestResponse, OpenConnectionRequest, OpenConnectionResponse}
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import rx.lang.scala.Observable

import rx.lang.scala.JavaConversions._
import scala.concurrent.duration._

class CouchbaseDCPReceiver(cfg: CouchbaseConfig) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  override def onStart() {
    val core = CouchbaseConnection().bucket(cfg, "default").core()

    val openConn = toScalaObservable(core.send[OpenConnectionResponse](new OpenConnectionRequest("sparkStream", "default")))
    val stream = openConn
      .flatMap(res => numPartitions(core))
      .flatMap(partitions => {

         Observable
            .from(0.until(partitions))
            .flatMap(partition => toScalaObservable(
              core.send[StreamRequestResponse](new StreamRequestRequest(partition.toShort, "default")))
            )
            .map(res => toScalaObservable(res.stream()))
            .flatten
      })

    stream.map(_.toString).subscribe(data => store(data))
  }

  override def onStop() {

  }

  def numPartitions(core: ClusterFacade): Observable[Int] = {
    toScalaObservable(core.send[GetClusterConfigResponse](new GetClusterConfigRequest()))
    .map(_.config().bucketConfig("default").asInstanceOf[CouchbaseBucketConfig].numberOfPartitions())
  }

}
