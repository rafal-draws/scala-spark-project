package analysers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CarSearch{
  val MANUFACTURER: String = "manufacturer"
  val MODEL: String = "model"
  val YEAR: String = "year"
  val DESCRIPTION: String = "description"
}

class CarSearch(sparkSession: SparkSession){

  def searchByManufacturer(keyword:String)(df:Dataset[Row]): Dataset[Row] ={
    df.filter(col(CarSearch.MANUFACTURER).contains(keyword))
  }

  def searchByManufacturerAndModel(manufacturer:String, model:String)(df:Dataset[Row]): Dataset[Row] ={
    df.filter(col(CarSearch.MANUFACTURER).contains(manufacturer) && col(CarSearch.MODEL).contains(model))
  }

  def searchDescByKeywords(keywords: Seq[String])(df:Dataset[Row]): Dataset[Row] ={
    df.withColumn("KeywordsResult", array_intersect(split(col(CarSearch.DESCRIPTION), " "), split(lit(keywords.mkString(",")), ",")))
      .filter(!(col("KeywordsResult").isNull.or(size(col("KeywordsResult")).equalTo(0))))
      .drop("KeywordsResult")
  }


}
