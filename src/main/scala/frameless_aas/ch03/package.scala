package frameless_aas

import scala.util.Random
import cats.Applicative
import cats.data.ReaderT
import cats.effect.{IO, Sync}
import cats.mtl.ApplicativeAsk
import cats.mtl.instances.local.askReader
import cats.syntax.flatMap._
import cats.syntax.functor._
import frameless.syntax._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import scala.reflect.runtime.universe.TypeTag

package object ch03 {
  type UserId   = Int
  type ArtistId = Int
  type Count    = Int
  type Prediction = Double

  type Action[T]      = ReaderT[IO, SparkSession, T]
  type SparkAsk[F[_]] = ApplicativeAsk[F, SparkSession]

  val UserID = 2093760

  implicit class BoolToOption(val self: Boolean) extends AnyVal {
    def toOption[A](value: => A): Option[A] =
      if (self) Some(value) else None
  }
  implicit val readerIOApplicativeAsk: SparkAsk[Action] =
    askReader[IO, SparkSession]

  def readLines[F[_]: Applicative, T <: Product: TypeTag: TypedEncoder](
    fileName: String, f: String => T)(implicit F: SparkAsk[F]
  ): F[TypedDataset[T]] = {
    def read(spark: SparkSession) = {
      import spark.implicits._
      spark.read.textFile(fileName).map(f)
    }
    F.ask.map(read(_).typed)
  }

  def flatReadLines[F[_]: Applicative, T <: Product: TypeTag](
    fileName: String,
    f: String => TraversableOnce[T]
  )(implicit F: SparkAsk[F], te: TypedEncoder[T]
  ): F[TypedDataset[T]] = {
    def read(spark: SparkSession) = {
      import spark.implicits.newProductEncoder
      spark.read.textFile(fileName).flatMap(f)
    }
    F.ask.map(spark => read(spark).typed)
  }
  trait Ch03Base {
    val path = "files/profiledata_06-May-2005"

    def program[F[_]: Applicative](
      f: (TypedDataset[UserArtistData], TypedDataset[ArtistData], TypedDataset[ArtistAlias]) => F[Unit])(
      implicit F: SparkAsk[F], S: Sync[F])
    : F[Unit] = for {
      playData <- readLines(s"$path/user_artist_data.txt", UserArtistData(_))
      artists  <- flatReadLines(s"$path/artist_data.txt", ArtistData(_))
      aliases  <- flatReadLines(s"$path/artist_alias.txt", ArtistAlias(_))
      _        <- f(playData, artists, aliases)
    } yield ()
  }
  def canonicalize[F[_]: Sync](
    playData: TypedDataset[UserArtistData],
    bAliases: Broadcast[Map[Int, Int]]
  ): TypedDataset[UserArtistData] = {
      val e = playData.makeUDF((n: Int) => bAliases.value.getOrElse(n, n))
      playData.withColumnReplaced('artistId, e(playData('artistId)))
    }
  def buildALSModel(rank: Int, regParam: Double, alpha: Double, ds: TypedDataset[_]): ALSModel =
    new ALS()
      .setSeed(Random.nextLong())
      .setImplicitPrefs(true)
      .setRank(rank)
      .setRegParam(regParam)
      .setAlpha(alpha)
      .setMaxIter(20)
      .setUserCol("userId")
      .setItemCol("artistId")
      .setRatingCol("playCount")
      .setPredictionCol("prediction")
      .fit(ds.dataset)

  def makeRecommendation[F[_]: Sync](
    model: ALSModel, userID: Int, howMany: Int)(
    implicit F: SparkAsk[F])
  : F[TypedDataset[ArtistPrediction]] =
    F.ask map { spark: SparkSession =>
      import spark.implicits._
      val toRecommend = model.itemFactors
        .select($"id".as("artistId"))
        .withColumn("userId", lit(userID))
      model.transform(toRecommend)
        .select("artistId", "prediction")
        .orderBy($"prediction".desc)
        .limit(howMany)
        .as[ArtistPrediction]
        .typed
    }
}
