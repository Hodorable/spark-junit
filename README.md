# Spark junit

Example of Spark tests using junit.

Create tests for spark code is not a easy task, specially if it is a spark streaming. I built some base classes to make it easier using junit. The sample code uses Gradle to manage build and dependencies.

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
class TestExample {
  import TestExample._
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

Create a test for a Spark Streaming code is much more difficult than for batch tests. The main challenge is to create a manageable clock where you can advance the virtual time to se the code executing as the tests run. Then we create a queue, in order to simulate the streaming order.
