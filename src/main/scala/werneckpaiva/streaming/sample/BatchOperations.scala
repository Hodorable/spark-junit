package werneckpaiva.streaming.sample

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import scala.reflect.runtime.universe

object BatchOperations {
  
  def isPair = udf((i:Int) => i%2 == 0)
  
  def countPairs(df:DataFrame):Long = {
    df.filter(isPair(df("_1"))).count
  }

  def countBlueEyes(df:DataFrame):Long = {
    df.filter(df("eyeColor")==="blue").count
  }
}