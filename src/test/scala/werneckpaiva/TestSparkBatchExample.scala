package werneckpaiva

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.junit.AfterClass
import org.junit.Assert._
import org.junit.BeforeClass
import org.junit.Test

object TestExample {
  var sc: SparkContext = null

  @BeforeClass
  def setup(): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Test Spark")
      .setMaster("local[*]")
    sc = new SparkContext(sparkConf)
  }

  @AfterClass
  def cleanup(): Unit = {
    sc.stop()
  }
}

class TestExample {

  @Test
  def testCountNumbers(): Unit = {
    val sql = new SQLContext(TestExample.sc)
    import sql.implicits._

    val numList = List(1, 2, 3, 4, 5)
    val df = TestExample.sc.parallelize(numList).toDF
    assertEquals(5, df.count)
  }

  @Test
  def testSaveMultipleTypes():Unit = {
    val sql = new SQLContext(TestExample.sc)
    import sql.implicits._

    val jsonRDD = TestExample.sc.parallelize(Seq(""" 
      { "isActive": false,
        "balance": 1431.73,
        "picture": "http://placehold.it/32x32",
        "age": 35,
        "eyeColor": "red"
      }""",
       """{
        "isActive": true,
        "balance": 2515.60,
        "picture": "http://placehold.it/32x32",
        "age": 34,
        "eyeColor": "blue"
      }""", 
      """{
        "isActive": false,
        "balance": 3765.29,
        "picture": "http://placehold.it/32x32",
        "age": 26,
        "eyeColor": "blue"
      }""")
    )

    val df = sql.read.json(jsonRDD)
    assertEquals(df.count, 3)
    val filteredDf = df.filter(df("eyeColor")==="blue")
    assertEquals(filteredDf.count, 2)
  }
}