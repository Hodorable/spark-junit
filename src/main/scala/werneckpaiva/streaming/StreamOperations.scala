package werneckpaiva.streaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration

object StreamOperations {

  def streamSum(stream:DStream[(String, Long)]) = stream.reduceByKey(_+_)
  
  def windowedSum(stream:DStream[(String, Long)], windowLength:Duration, slidingInterval:Duration) = {
    stream.reduceByKeyAndWindow(_+_, _-_, windowLength, slidingInterval)
  }

}