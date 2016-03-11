package werneckpaiva.spark.test.util

import scala.reflect.ClassTag
import org.apache.spark.streaming.StreamingContext
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.Time


class TestInputStream[T: ClassTag](
    ssc: StreamingContext, 
    val queue: Queue[RDD[T]],
    defaultRDD: RDD[T])
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