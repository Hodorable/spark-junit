# Spark junit

Example of Spark tests using junit.

Create tests for spark code is not a easy task, specially if it is a spark streaming. I built some base classes to make it easier using junit. The sample code uses Gradle to manage build and dependencies.

## Spark batch
To run spark batch tests you need to start a local sparkContext at the beginning of all tests and stop it at the end.

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

Your tests should first create a Dataframe using parallelize method from the SparkContext. If your code is based o dataframe, you can use the method toDF available when you imported the SQLContext implicities.

```scala
@Test
def testCountPairNumbers(): Unit = {
  val df =  sc.parallelize(List(1, 2, 3, 4, 5, 6)).toDF

  val count = MyOperations.countPairs(df)

  assertEquals(3, count)
}
```

## Spark Streaming
