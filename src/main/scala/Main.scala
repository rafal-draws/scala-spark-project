import analysers.{CarAnalyzer, CarSearch}
import cleaners.CarDataCleaner
import loaders.CarDataLoader
import loaders.CarDataLoader._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Main {
  /**
   * 1. Preview data, analyze connections
   * 2. Load the data
   * 3. Clean and prepare the data
   * 4. Connect the data
   * 5. Analyze the data
   */

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark-fundamentals")
      .master("local[*]")
      .getOrCreate()

    val carDataLoader: CarDataLoader = new CarDataLoader(spark)
    val carDataCleaner: CarDataCleaner = new CarDataCleaner(spark)
    val carSearch: CarSearch = new CarSearch(spark)
    import carSearch._
    val carAnalyzer: CarAnalyzer = new CarAnalyzer(spark)


    val DF: Dataset[Row] = carDataLoader.loadAll().cache()
    val cleanedDF: Dataset[Row] = carDataCleaner.cleanUsedCars(DF)

    // Car Search
    val bmwDF: Dataset[Row] = cleanedDF.transform(searchByManufacturer("BMW"))
//    bmwDF.show()

    val interestingAndLuxuriousDF: Dataset[Row] = cleanedDF
      .transform(searchByManufacturer("Lexus"))
      .transform(searchDescByKeywords(Seq("interesting", "luxurious")))
//    interestingAndLuxuriousDF.show()

    // Car Analysis

    val v8after2016DF: Dataset[Row] = carAnalyzer.calculateV8sFrom2016(cleanedDF)
//    v8after2016DF.show()

    val affordableYoungtimersDF: Dataset[Row] = carAnalyzer.affordableYoungtimers(cleanedDF)
//    affordableYoungtimersDF.show()

    val mostPopularPaintsDF: Dataset[Row] = carAnalyzer.eachStateMostPopularPaints(cleanedDF)
//    mostPopularPaints.show()

    val avaragePricesForStateDF: Dataset[Row] = carAnalyzer.calculateAvgCarPricePerLocation(cleanedDF)
    avaragePricesForStateDF.show()


  }

}
