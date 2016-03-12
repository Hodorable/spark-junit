# Spark junit

Example of Spark tests using junit.

To create tests for Spark code is not an easy task, specially if it is a spark streaming. I built some base classes to make it easier using junit. The sample code uses Gradle to manage build and dependencies.

## Spark batch
To run spark batch tests you need to start a local SparkContext at the beginning of all tests and stop it at the end.

First create an object that starts and stops SparkContext:
```scala
object TestMyCode {
  var sc: SparkContext = _

  @BeforeClass
  def before(): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Test Spark Batch")
      .setMaster("local")
    sc = new SparkContext(sparkConf)
  }

  @AfterClass
  def after(): Unit = {
    sc.stop()
  }
}
```

Create the class with your tests. Start the class making the SparkContext available to this class and create an SQLContext based on that.

```scala
class TestMyCode {
  import TestMyCode._
  val sql = new SQLContext(sc)
  import sql.implicits._

  // Your tests go here
```

Your tests should first create a RDD using the parallelize method from the SparkContext. If your code is based on DataFrames, you can use the method toDF, available when you imported the SQLContext implicits.

```scala
@Test
def testCountPairNumbers(): Unit = {
  val df =  sc.parallelize(List(1, 2, 3, 4, 5, 6)).toDF

  val count = MyOperations.countPairs(df)

  assertEquals(3, count)
}
```

## Spark Streaming

To create a test for a Spark Streaming code is much more difficult than for batch tests. The main challenge is to create a manageable clock where you can advance the virtual time, so the code executes as the tests run. Then we create a queue, in order to simulate the streaming order.

I created 2 classes that overwrite the regular streaming behavior: `org.apache.spark.ClockWrapper` and `org.apache.spark.streaming.StreamingContextWrapper`. Another class that creates a DStream based on a queue: `werneckpaiva.spark.test.util.TestInputDStream`. And a last utility class to start and stop streaming and manage the clock asynchronicity: `werneckpaiva.spark.test.BaseTestSparkStreaming`.


Usage:
```scala
class TestSparkStreaming extends BaseTestSparkStreaming{

  @Test
  def testSimpleSum():Unit = {
    val (lines, stream) = makeStream[(String, Long)]()

    # Code you want to test
    val reducedStream = StreamOperations.streamSum(stream)

    lines += sc.makeRDD(Seq(("a", 1L), ("a", 1L), ("b", 1L)))
    lines += sc.makeRDD(Seq(("a", 1L), ("a", 1L)))
    lines += sc.makeRDD(Seq(("a", 1L), ("a", 2L), ("b", 3L), ("b", 2L)))

    val results = waitForResultReady(reducedStream, Seconds(20))

    assertEquals(("a", 2), results(0)(0))
    assertEquals(("b", 1), results(0)(1))
  }
}
```
