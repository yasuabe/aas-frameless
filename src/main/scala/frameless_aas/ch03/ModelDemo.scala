package frameless_aas.ch03

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._
import frameless.TypedDataset
import frameless.cats.implicits._
import org.apache.spark.ml.recommendation.ALSModel
import frameless_aas._
import frameless_aas.broadcast

trait ModelDemo[F[_]] {
  val SampleUserID = 2093760
  implicit val F: SparkAsk[F]
  implicit val S: Sync[F]

  private def print(s: Any) = S.delay(println(s))

  def model(
    userArtists: TypedDataset[UserArtistData],
    artists: TypedDataset[ArtistData],
    aliases: TypedDataset[ArtistAlias]
  ): F[Unit] = for {
    _           <- print(s"artists $SampleUserID has played")
    bAliases    <- ArtistAlias.canonicalMap(aliases) >>= broadcast[F, Map[Int, Int]]
    trainData   =  canonicalize(userArtists, bAliases)
    model       <- trainALSModel(trainData)
    playedByHim <- selectArtists(trainData, SampleUserID).collect[F]
    _           <- artists.filter(artists('id).isin(playedByHim:_*)).show()

    _           <- print(s"recommendations for $SampleUserID")
    recommended <- makeRecommendation(model, SampleUserID, 5)
    _           <- recommended.show()

    _         <- print(s"recommendations for $SampleUserID")
    artistIds <- recommended.select(recommended('artistId)).as[Int].collect()
    _         <- artists.filter(artists('id) isin (artistIds:_*)).show()
  } yield {
    model.userFactors.unpersist()
    model.itemFactors.unpersist()
  }

  def selectArtists(trainData: TypedDataset[UserArtistData], userId: Int): TypedDataset[Int] =
    trainData
    .filter(trainData('userId) === userId)
    .select(trainData('artistId))
    .as[Int]

  def trainALSModel(trainData: TypedDataset[UserArtistData]): F[ALSModel] =
    useCache(trainData)(buildALSModel(10, 0.01, 1.0, _))
}
object ModelDemoMain extends Ch03Base with IOApp with UsesSparkSession[IO] {
  val S: Sync[IO] = implicitly[Sync[IO]]
  private val instance = new ModelDemo[Action] {
    val F: SparkAsk[Action] = implicitly[SparkAsk[Action]]
    val S: Sync[Action] = implicitly[Sync[Action]]
  }
  def run(args: List[String]): IO[ExitCode] =
    useSpark(program[Action](instance.model).run) as ExitCode.Success
}

/*

$ export SBT_OPTS="-XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=48G -Xmx48G"
$ sbt console

spark.conf.set("spark.sql.crossJoin.enabled", "true")

import cats.data.ReaderT
import cats.effect.IO
import frameless_aas.ch03.ModelDemo
import cats.mtl.ApplicativeAsk
import cats.effect.Sync
import frameless_aas.ch03.readerIOApplicativeAsk

type Action[T] = ReaderT[IO, SparkSession, T]

val instance = new ModelDemo[Action] {
  val F: ApplicativeAsk[Action, SparkSession] = implicitly[ApplicativeAsk[Action, SparkSession]]
  val S: Sync[Action] = implicitly[Sync[Action]]
}
import frameless_aas.ch03.UserArtistData
import frameless_aas.ch03.ArtistData
import frameless_aas.ch03.ArtistAlias
import frameless_aas.ch03.readLines
val path = "files/profiledata_06-May-2005"
val played = readLines[Action, UserArtistData](s"$path/user_artist_data.txt", UserArtistData(_)).run(spark).unsafeRunSync

import frameless_aas.ch03.flatReadLines
val artists = flatReadLines[Action, ArtistData](s"$path/artist_data.txt", ArtistData(_)).run(spark).unsafeRunSync
val aliases = flatReadLines[Action, ArtistAlias](s"$path/artist_alias.txt", ArtistAlias(_)).run(spark).unsafeRunSync

instance.model(played, artists, aliases).run(spark).unsafeRunSync


artists 2093760 has played
+-------+---------------+
|     id|           name|
+-------+---------------+
|   1180|     David Gray|
|    378|  Blackalicious|
|    813|     Jurassic 5|
|1255340|The Saw Doctors|
|    942|         Xzibit|
+-------+---------------+

recommendations for 2093760
+--------+-----------+
|artistId| prediction|
+--------+-----------+
|    2814| 0.03348194|
| 1300642| 0.03296377|
|    4605|0.032598738|
| 1007614|0.032298084|
| 1037970|0.032248717|
+--------+-----------+

recommendations for 2093760
+-------+----------+
|     id|      name|
+-------+----------+
|   2814|   50 Cent|
|   4605|Snoop Dogg|
|1007614|     Jay-Z|
|1037970|Kanye West|
|1300642|  The Game|
+-------+----------+
*/
