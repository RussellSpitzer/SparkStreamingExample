package com.datastax.spark.example

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.{Partition, SparkContext, TaskContext}

case class TheRow(key: Int, ckey: Int, value: String)

class TheStream(
  _ssc: StreamingContext,
  duration: Duration,
  initialState: Int = 0) extends InputDStream[TheRow](_ssc) {

  var myState = initialState // For checkpointing

  override def slideDuration: Duration = duration

  override def dependencies: List[DStream[_]] = List.empty

  override def compute(validTime: Time): Option[RDD[TheRow]] = {
    myState += 1
    Some(new TheRDD(context.sparkContext, myState))
  }

  override def start(): Unit = {
    logInfo("Starting The Stream")
  }

  override def stop(): Unit = {
    logInfo("Stopping The Stream")
  }
}

class TheRDD(sparkContext: SparkContext, state: Int) extends RDD[TheRow](sparkContext, Seq.empty){
  override def compute(split: Partition, context: TaskContext): Iterator[TheRow] = {
    val partitionNum = split.index
    (1 to 10).iterator.map( i => TheRow( i + partitionNum * 10 + 10000 * state, i + partitionNum * 10, i.toString ))
  }

  override protected def getPartitions: Array[Partition] =
    (1 to 10).map(partitionNum => new Partition {override def index: Int = partitionNum}).toArray
}
