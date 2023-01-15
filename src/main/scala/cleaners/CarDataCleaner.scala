package cleaners

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class CarDataCleaner(sparkSession: SparkSession){

  def cleanUsedCars(df: Dataset[Row]): Dataset[Row] ={

    df
      .na.drop(Seq("manufacturer", "model"))
      .na.fill("not disclosed")
      .withColumn("description", regexp_replace(col("description"), "\\]", ""))
      .withColumn("description", regexp_replace(col("description"), "\\[", ""))
      .withColumn("description", regexp_replace(col("description"), "!@@Additional Info@@!", ""))
      .withColumn("type", regexp_replace(col("type"), "-121.7473", "null"))
      .withColumn("type", regexp_replace(col("type"), "645", "null"))
      .withColumn("price", col("price").cast(DataTypes.DoubleType))
      .withColumn("date", col("date").cast(DataTypes.DateType))
      .withColumn("year", col("year").cast(DataTypes.IntegerType))
      .withColumn("mileage", col("mileage").cast(DataTypes.LongType))
      .withColumn("latitude", col("latitude").cast(DataTypes.DoubleType))
      .withColumn("longitude", col("longitude").cast(DataTypes.DoubleType))
      .drop("state")
  }


}
