package werneckpaiva.spark.test.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.junit.AfterClass
import org.junit.Assert._
import org.junit.BeforeClass
import org.junit.Test

import werneckpaiva.streaming.sample.BatchOperations;

object TestExample {
  var sc: SparkContext = null

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

class TestExample {
  import TestExample._
  val sql = new SQLContext(sc)
  import sql.implicits._

  @Test
  def testCountPairNumbers(): Unit = {
    val df =  sc.parallelize(List(1, 2, 3, 4, 5, 6)).toDF

    val count = BatchOperations.countPairs(df)

    assertEquals(3, count)
  }

  @Test
  def testSaveMultipleTypes():Unit = {
    val jsonRDD = sc.parallelize(Seq(""" 
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

    val count = BatchOperations.countBlueEyes(df)
    assertEquals(count, 2)
  }
}