package com.epam.sparkbasics

import ch.hsr.geohash.GeoHash.withCharacterPrecision
import com.opencagedata.geocoder.OpenCageClient
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{Row, SparkSession}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object SparkBasics extends Serializable {
  def main(args: Array[String]): Unit = {
//  Creating Spark configuration
    val conf = new SparkConf().setAppName("SparkBasics")

//  Setting parametres for connection with Azure ADLS gen2 storages
    conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    conf.set("fs.azure.account.key.kkladls.blob.core.windows.net", "BqwznS7iDuURkFWwe7yduzkCkwTHyIGRq0kSoazy87e8jOzwLsyTW42Ty6kPfCGCH9ImCyjeLq0vua2ESmeZUA==")
    conf.set("fs.azure.account.key.bdccstorageacc.blob.core.windows.net", "TgKk9CtmHgZ3RB4+un9PQqnyX9D1k0OEPJy0J99cFYrLvFKN5eN/0+sptNlrM41YCyqKfrv8XJPL38qhGXn1Qg==")

    //  Creating Spark session and run spark job
    val spark = SparkSession.builder
      .appName("SparkBasics")
      .config(conf)
      .getOrCreate()

//  Reading hotels data from storage container testcontainers@kkladls
    var hotels_df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("wasbs://testcontainers@kkladls.blob.core.windows.net/hotels/")

//  Mapping Latitude & Longitude for incorrect values using OpenCage Geocoding API
//  Creating OpenCageClient with API key
    val client = new OpenCageClient("9d9b253d364c4127a4ac10308ef59ffe")

      hotels_df
        //    Check hotels data on incorrect (null) values (Latitude & Longitude)
        .filter((hotels_df("Latitude").isNull) || (hotels_df("Longitude").isNull) || (hotels_df("Latitude") === "NA") || (hotels_df("Longitude") === "NA"))
        .foreach(row => {
            val address = row(4)
            val city = row(3)
            val country = row(2)
            val query = address + "," + city + "," + country

            //   Getting latitude and longitude from request with query string contented a address
            val responseFuture = client.forwardGeocode(query)
            val response = Await.result(responseFuture, 5.seconds)
            val location = response.results.head.geometry

            //      Changing value of Latitude and Longitude columns using a transformation function withColumn()
            hotels_df = hotels_df.withColumn("Latitude",
                when(col("Address") === address && col("City") === city && col("Country") === country, location.head.lat)
                  .otherwise(col("Latitude")))
            hotels_df = hotels_df.withColumn("Longitude",
                when(col("Address") === address && col("City") === city && col("Country") === country, location.head.lng)
                  .otherwise(col("Longitude")))
        })

//  Closing the client
    client.close()

//  Creating column "GeoHashHotels" for generated geohash by Latitude & Longitude
    hotels_df = hotels_df.withColumn("GeoHashHotels", lit("null"))

    val encoder_hotels = RowEncoder(hotels_df.schema)
//  Generating geohash by Latitude & Longitude for hotels DataFrame
    hotels_df = hotels_df
      .map(row => {
        val lat = row(5).toString.toDouble
        val lng = row(6).toString.toDouble
        val geohashString = withCharacterPrecision(lat, lng, 4).toBase32()
        Row(row(0), row(1), row(2), row(3), row(4), row(5), row(6), geohashString)
      })(encoder_hotels)

//  Reading weather data from storage container testcontainers@kkladls into DataFrame weather_df
    var weather_df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("wasbs://testcontainers@kkladls.blob.core.windows.net/weather")

//  Creating column "GeoHashWeather" for generated geohash by Latitude & Longitude
    weather_df = weather_df.withColumn("GeoHashWeather", lit("null"))

    val encoder_weather = RowEncoder(weather_df.schema)
//  Generating geohash by Latitude & Longitude for weather DataFrame
    weather_df = weather_df
      .map(row => {
        val lat = row(1).toString.toDouble
        val lng = row(0).toString.toDouble
        val geohashString = withCharacterPrecision(lat, lng, 4).toBase32()
        Row(row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7), geohashString)
      })(encoder_weather)

//  Left join weather and hotels data by generated 4-characters geohash
    val left_join_df = weather_df.join(hotels_df, weather_df("GeoHashWeather") === hotels_df("GeoHashHotels"), "left")
//  Writing enriched data (joined data with all the fields from both datasets) in provisioned with terraform Azure ADLS gen2 storage preserving data partitioning in parquet format in “data” container
    left_join_df.write
      .option("header", "true")
      .option("inferSchema", "true")
      .mode("overwrite")
      .parquet("wasbs://data@bdccstorageacc.blob.core.windows.net/left_join")
  }
}
