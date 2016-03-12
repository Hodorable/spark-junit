package werneckpaiva.spark.test.util

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.InputDStream


class TestInputDStream[T:ClassTag](
    ssc: StreamingContext, queue: Queue[RDD[T]], defaultRDD: RDD[T])
      extends InputDStream[T](ssc) {

  def start() {}

  def stop() {}

  def compute(validTime: Time): Option[RDD[T]] = {
    val buffer = new ArrayBuffer[RDD[T]]()
    if (!queue.isEmpty) {
      Some(queue.dequeue())
    } else {
      Some(defaultRDD)
    }
  }
}