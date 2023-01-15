package analysers

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CarAnalyzer{
  val VIN: String = "vin"
  val TYPE: String = "type"
  val MANUFACTURER: String = "manufacturer"
  val MODEL: String = "model"
  val PRICE: String = "price"
  val YEAR: String = "year"
  val CONDITION: String = "condition"
  val CYLINDERS: String = "cylinders"
  val FUEL: String = "fuel"
  val BODY_PAINT: String = "body_paint"
  val MILEAGE: String = "mileage"
  val DESCRIPTION: String = "description"
  val DATE: String = "date"
  val SOURCE: String = "source"
  val CODE: String = "code"
  val LATITUDE: String = "latitude"
  val LONGITUDE: String = "longitude"
}

class CarAnalyzer(sparkSession: SparkSession) {

  /**
   * @param df
   * @return Dataframe of car ads counting whether cars meet the V8 and above 2016 year mark or not
   */


  def calculateV8sFrom2016(df:Dataset[Row]):Dataset[Row] ={
    df.withColumn("V8 after 2016", col(CarAnalyzer.CYLINDERS).equalTo("V8") && col(CarAnalyzer.YEAR).geq(lit(2016)))
      .groupBy(col("V8 after 2016")).count()
  }

  /**
   * @param df
   * @return Dataframe of cars below $10_000 and 1996y mark
   */

  def affordableYoungtimers(df:Dataset[Row]):Dataset[Row]={
    df.filter(col(CarAnalyzer.YEAR).leq(lit(1996)))
      .filter(col(CarAnalyzer.PRICE).leq(lit(10000)))
  }

  /**
   * @param df
   * @return Dataframe of most popular paints with state codes
   */

  def eachStateMostPopularPaints(df:Dataset[Row]): Dataset[Row] ={
    df.filter(!col(CarAnalyzer.CODE).contains("not disclosed"))
      .filter(!col(CarAnalyzer.BODY_PAINT).contains("not disclosed"))
      .groupBy(CarAnalyzer.CODE, CarAnalyzer.BODY_PAINT).count()
      .orderBy(col("count").desc)
  }

  def calculateAvgCarPricePerLocation(df:Dataset[Row]): Dataset[Row] ={
    df.groupBy(CarAnalyzer.CODE)
      .avg(CarAnalyzer.PRICE)
      .as("avarage_price")

  }


}
