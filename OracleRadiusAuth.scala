import java.util.Properties
import org.apache.spark.sql.SparkSession


def main(args: Array[String]): Unit ={
	val spark = SparkSession
      .builder()
      .appName("SparkRadiusAuthToOracle")
      .getOrCreate()
	
	val connectionProperties = new Properties()
	connectionProperties.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES,
      "("+AnoServices.AUTHENTICATION_RADIUS+")");
    connectionProperties.setProperty("user", "username")
    connectionProperties.setProperty("password", "password")
	val url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)"+ "(HOST=oracleserver.mydomain.com)(PORT=5561))(CONNECT_DATA=" + "(SERVICE_NAME=mydatabaseinstance)))
	val jdbcDF2 = spark.read.jdbc(url, "schema.tablename", connectionProperties)
}
