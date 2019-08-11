package frameless_aas.ch03

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._
import frameless.TypedDataset
import frameless.cats.implicits._
import frameless_aas._

trait RecommendDemo[F[_]] {
  implicit val F: SparkAsk[F]
  implicit val S: Sync[F]

  def recommend(
    userArtists: TypedDataset[UserArtistData],
    artistData:  TypedDataset[ArtistData],
    aliases:     TypedDataset[ArtistAlias]
  ): F[Unit] = for {
    bAliases <- ArtistAlias.canonicalMap(aliases) >>= broadcast[F, Map[Int, Int]]
    aliasMap =  canonicalize(userArtists, bAliases)
    model    <- useCacheM(aliasMap) { all => S.pure(buildALSModel(10, 1.0, 40, all)) }

    top5 <- makeRecommendation(model, UserID, 5)
    j    =  artistData.joinInner(top5)(artistData('id) === top5('artistId))
    _    <- j.select(j.colMany('_1, 'name)).show()

    _ <- unpersistF(model.userFactors) >> unpersistF(model.itemFactors)
  } yield ()
}
object RecommendDemoMain extends Ch03Base with IOApp with UsesSparkSession[IO] {
  val S: Sync[IO] = implicitly[Sync[IO]]
  private val instance = new RecommendDemo[Action] {
    val F: SparkAsk[Action] = implicitly[SparkAsk[Action]]
    val S: Sync[Action] = implicitly[Sync[Action]]
  }
  def run(args: List[String]): IO[ExitCode] =
    useSpark(program[Action](instance.recommend).run) as ExitCode.Success
}

