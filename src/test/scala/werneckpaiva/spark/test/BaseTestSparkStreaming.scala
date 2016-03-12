package werneckpaiva.spark.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.After
import org.junit.Before
import org.apache.spark.ClockWrapper
import org.apache.spark.streaming.Seconds
import java.nio.file.Files
import scala.reflect.ClassTag
import werneckpaiva.spark.test.util.TestInputDStream
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration

class BaseTestSparkStreaming {
  var ssc:StreamingContext = _
  var sc:SparkContext = _
  var clock:ClockWrapper = _

  @Before
  def before():Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Test Spark Streaming")
      .setMaster("local[*]")
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

    val checkpointDir = Files.createTempDirectory("test").toString
    ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(checkpointDir)
    sc = ssc.sparkContext
    clock = new ClockWrapper(ssc)
  }

  @After
  def after():Unit = {
    ssc.stop()
    sc.stop()
  }

  def makeStream[T:ClassTag]():(Queue[RDD[T]], TestInputDStream[T]) = {
    val lines = new Queue[RDD[T]]()
    val stream = new TestInputDStream[T](ssc, lines, sc.makeRDD(Seq[T](), 1))
    (lines, stream)
  }
  
  def waitForResultReady[T](stream:DStream[T], time:Duration):ListBuffer[Array[T]] = {
    val results = ListBuffer.empty[Array[T]]
    stream.foreachRDD((rdd, time) => {
      results.append(rdd.collect()) 
    })
    ssc.start()
    clock.advance(time.milliseconds)
    for(i <- 1 to 100){
      if(results.length >= 1) return results
      Thread.sleep(100)
    }
    throw new Exception("Can't load stream")
  }
}