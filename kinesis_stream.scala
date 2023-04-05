import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable

case class DimData(id: Int, name: String, value: String)

object StreamJoinWithInMemorySCD {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StreamJoinWithInMemorySCD")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample slowly changing dimension data
    val scdData = Seq(
      DimData(1, "A", "Value1"),
      DimData(2, "B", "Value2"),
      DimData(3, "C", "Value3")
    )

    // In-memory map to store slowly changing dimension data
    val scdMap = mutable.Map.empty[Int, DimData]
    scdData.foreach { data =>
      scdMap(data.id) = data
    }

    // Define the schema of the streaming data
    val schema = Seq("id", "value").toDF("id", "value").schema

    // Create a streaming DataFrame from a socket source
    val streamingDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .select(from_json($"value", schema).as("data"))
      .selectExpr("data.id", "data.value")

    // Join the streaming DataFrame with the in-memory map of slowly changing dimension data
    val joinedStream = streamingDF.mapPartitions(partition => {
      partition.flatMap(row => {
        val id = row.getInt(0)
        scdMap.get(id).map(d => (id, d.name, row.getString(1)))
      })
    }).toDF("id", "name", "value")

    // Start the query
    val query: StreamingQuery = joinedStream.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

    spark.stop()
  }
}
