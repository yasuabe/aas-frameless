import cats.effect.Sync
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object frameless_aas {
  trait UsesSparkSession[F[_]] {
    protected val S: Sync[F]
    private def acquire: F[SparkSession] = {
      val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("frameless-first-example")
        .set("spark.ui.enabled", "false")
        .set("spark.driver.memory", "32g")
        .set("spark.sql.crossJoin.enabled", "true")

      implicit val spark: SparkSession = SparkSession.builder()
        .config(conf)
        .appName("aas-frameless")
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")
      S.pure(spark)
    }
    private def release(spark: SparkSession): F[Unit] = S.delay(spark.stop())
    def useSpark(use: SparkSession => F[Unit]): F[Unit] =
      S.bracket(acquire)(use)(release)
  }
}
