package com.datastax.spark.example

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


// For DSE it is not necessary to set connection parameters for spark.master (since it will be done
// automatically)
object StreamExample extends App {

  val keyspace = "ks"
  val table = "therows"
  val checkpointDir = "dsefs:///therows/"

  val conf = new SparkConf()
    .setAppName("An Exercise in Streaming")

  CassandraConnector(conf).withSessionDo { session =>
    session.execute(
      s"""CREATE KEYSPACE IF NOT EXISTS $keyspace WITH
        | replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }""".stripMargin)
    session.execute(s"""CREATE TABLE IF NOT EXISTS $keyspace.$table (key int, ckey int, value text, PRIMARY KEY (key, ckey))""")
  }

  // A SparkContext
  val sc = new SparkContext(conf)

  val ssc = StreamingContext.getActiveOrCreate(checkpointDir, setupStreamingContext)

  def setupStreamingContext() = {
    val ssc = new StreamingContext(sc, Duration(1000))
    val dstream = new TheStream(ssc, Duration(1000))
    dstream.saveToCassandra(keyspace, table)
    dstream.print()
    ssc.checkpoint(checkpointDir)
    ssc
  }

  ssc.start()
  try {
    ssc.awaitTermination()
  } catch {
    case e : Throwable =>
      ssc.stop(true, false) // JUST SHUT DOWN
      throw e
  }

  sys.exit(0)
}
