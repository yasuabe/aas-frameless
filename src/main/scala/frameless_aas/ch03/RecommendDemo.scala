package frameless_aas.ch03

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.mtl.ApplicativeAsk
import cats.syntax.flatMap._
import cats.syntax.functor._
import frameless.TypedDataset
import frameless.cats.implicits._
import frameless_aas._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
import scala.util.Random

trait RecommendDemo[F[_]] {
  implicit val F: ApplicativeAsk[F, SparkSession]
  implicit val S: Sync[F]
  import S._

  val UserID = 2093760

  def broadcast[T: ClassTag](value: T): F[Broadcast[T]] =
    F.ask.map(_.sparkContext.broadcast(value))

  def recommend2( // TODO rename
    userArtists: TypedDataset[UserArtistData],
    artistData: TypedDataset[ArtistData],
    aliases: TypedDataset[ArtistAlias]
  ): F[Unit] = for {
    model <- cacheUnpersist(canonicalize(userArtists, aliases)) { allData =>
               S.pure(buildALSModel(10, 1.0, 40, allData))
             }
    top5 <- recommend(model, UserID, 5)

    j = artistData.joinInner(top5)(artistData('id) === top5('artistId))
    _ <- j.select(j.colMany('_1, 'name)).show()

    _ <- unpersistF(model.userFactors) >> unpersistF(model.itemFactors)
  } yield ()

  // TODO: DuplicateCode
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
}
object RecommendDemoMain extends Ch03Base with IOApp with UsesSparkSession[IO] {
  val S: Sync[IO] = implicitly[Sync[IO]]
  private val instance = new RecommendDemo[Action] {
    val F: ApplicativeAsk[Action, SparkSession] = implicitly[ApplicativeAsk[Action, SparkSession]]
    val S: Sync[Action] = implicitly[Sync[Action]]
  }
  def run(args: List[String]): IO[ExitCode] =
    useSpark(program[Action](instance.recommend2).run) as ExitCode.Success
}

