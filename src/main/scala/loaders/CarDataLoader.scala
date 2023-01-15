package loaders

import org.apache.spark.sql.functions.{col, lit, lower}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CarDataLoader{
  val CRAIGSLIST_LABEL: String = "craigslist"
  val CARGURU_LABEL: String = "carguru"
}


class CarDataLoader (sparkSession: SparkSession){

  def loadAll(): Dataset[Row]={
    val craigslistDF = loadCraigslistData()
    val carguruDF = loadCarguruData()
    val geodataDF = loadGeodata()

    val craigslistWithGeoDataDF = craigslistDF.join(geodataDF, "state")

    craigslistWithGeoDataDF.unionByName(carguruDF, allowMissingColumns = true)
  }

  def loadCraigslistData(): Dataset[Row] ={
    sparkSession.read.option("header", "true")
      .csv("vehicles.csv")
      .select(col("VIN").as("vin"),
        col("type"),
        col("manufacturer"),
        col("model"),
        col("price"),
        col("year"),
        col("condition"),
        col("cylinders"),
        col("fuel"),
        col("paint_color").as("body_paint"),
        col("odometer").as("mileage"),
        col("description"),
        col("state"),
        col("posting_date").as("date"))
      .withColumn("source", lit(CarDataLoader.CRAIGSLIST_LABEL))

  }

  def loadCarguruData(): Dataset[Row] ={
    sparkSession.read.option("header", "true")
      .csv("used_cars_data.csv")
      .select(col("vin"),
        col("body_type").as("type"),
        col("make_name").as("manufacturer"),
        col("model_name").as("model"),
        col("price"),
        col("year"),
        // COLUMNS FOR CONDITION ANALYSIS
        //TODO

        // DETAILS
        col("engine_cylinders").as("cylinders"),
        col("fuel_type").as("fuel"),
        col("exterior_color").as("body_paint"),
        col("mileage"),
        col("description"),

        col("latitude"),
        col("longitude"),

        col("listed_date").as("date")

      )
      .withColumn("source", lit(CarDataLoader.CARGURU_LABEL))
  }

  def loadGeodata(): Dataset[Row]={

    val stateNamesAndShortsSchema = new StructType()
      .add("state", StringType,nullable = false)
      .add("abbrev", StringType,nullable = false)
      .add("code", StringType,nullable = false)

    val stateNamesAndShortsDF: Dataset[Row] = sparkSession
      .read
      .option("multiline", "true")
      .schema(stateNamesAndShortsSchema)
      .json("statenamesandshorts.json")

    val usStatesAvgLatLongSchema = new StructType()
      .add("state", StringType, nullable = false)
      .add("latitude", DoubleType, nullable = false)
      .add("longitude", DoubleType, nullable = false)

    val usStatesAvgLatLongDF: Dataset[Row] = sparkSession
      .read
      .option("multiline", "true")
      .schema(usStatesAvgLatLongSchema)
      .json("USstates_avg_latLong.json")

    val joinedAndSelectedDF: Dataset[Row] = usStatesAvgLatLongDF.join(stateNamesAndShortsDF, "state")
      .withColumn("code", lower(col("code")))
      .select("code", "latitude", "longitude")

    val referenceDF: Dataset[Row] = joinedAndSelectedDF.withColumn("state", col("code"))

    referenceDF


  }

}
