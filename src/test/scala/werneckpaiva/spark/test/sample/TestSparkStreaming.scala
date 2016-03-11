package werneckpaiva.spark.test.sample

import org.apache.spark.streaming.Seconds
import org.junit.Assert._
import org.junit.Test

import werneckpaiva.spark.test.BaseTestSparkStreaming
import werneckpaiva.streaming.sample.StreamOperations


class TestSparkStreaming extends BaseTestSparkStreaming{

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