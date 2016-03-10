package werneckpaiva.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.Before
import org.junit.Assert._
import org.junit.Test
import org.junit.After
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.ClockWrapper
import org.apache.spark.rdd.RDD
import werneckpaiva.util.TestInputStream
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import java.nio.file.Files


class TestSparkStreaming {

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

  def makeStream[T:ClassTag]():(Queue[RDD[T]], TestInputStream[T]) = {
    val lines = new Queue[RDD[T]]()
    val stream = new TestInputStream[T](ssc, lines, sc.makeRDD(Seq[T](), 1))
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

  @Test
  def testSimpleSum():Unit = {
    val (lines, stream) = makeStream[(String, Long)]()

    val reducedStream = StreamOperations.streamSum(stream)

    lines += sc.makeRDD(Seq(("a", 1L), ("a", 1L), ("b", 1L)))
    lines += sc.makeRDD(Seq(("a", 1L), ("a", 1L)))
    lines += sc.makeRDD(Seq(("a", 1L), ("a", 2L), ("b", 3L), ("b", 2L)))

    val results = waitForResultReady(reducedStream, Seconds(20))

    assertEquals(("a", 2), results(0)(0))
    assertEquals(("b", 1), results(0)(1))
  }

  @Test
  def testWindowedSum_firstWindow():Unit = {
    val (lines, stream) = makeStream[(String, Long)]()

    val reducedStream = StreamOperations.windowedSum(stream, Seconds(60), Seconds(30))

    lines += sc.makeRDD(Seq(("a", 1L), ("a", 1L), ("b", 1L)))
    lines += sc.makeRDD(Seq(("a", 1L), ("a", 1L)))
    lines += sc.makeRDD(Seq(("a", 1L), ("a", 2L), ("b", 3L), ("b", 2L)))

    val results = waitForResultReady(reducedStream, Seconds(30))

    assertEquals(("a", 7), results(0)(0))
    assertEquals(("b", 6), results(0)(1))
  }
}