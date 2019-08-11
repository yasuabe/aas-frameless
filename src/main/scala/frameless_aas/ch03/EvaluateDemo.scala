package frameless_aas.ch03

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.instances.list._
import cats.mtl.ApplicativeAsk
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import frameless.TypedDataset
import frameless.cats.implicits._
import frameless.functions.aggregate._
import frameless_aas._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.types.DoubleType

trait EvaluateDemo[F[_]] {
  implicit val F: SparkAsk[F]
  implicit val S: Sync[F]
  import S._

  private def print(s: Any) = S.delay(println(s))

  def evaluate(
    userArtists: TypedDataset[UserArtistData],
    artistData: TypedDataset[ArtistData],
    aliases: TypedDataset[ArtistAlias]
  ): F[Unit] = {
    for {
      bAliases      <- ArtistAlias.canonicalMap(aliases) >>= broadcast[F, Map[Int, Int]]
      all           =  canonicalize(userArtists, bAliases)
      bAllArtistIDs <- all.select(all('artistId)).distinct.collect() >>= broadcast[F, Seq[Int]]

      Array(train, cv) =  all.randomSplit(Array(0.9, 0.1))
      _                <- cache(train) >> cache(cv)

      mostListenedAUC  <- areaUnderCurve(cv, bAllArtistIDs, predictMostListened(train))
      _                <- print(mostListenedAUC)

      evaluated <- tryCombinations(train)(aucFunc(cv, bAllArtistIDs)).sequence
      _         <- evaluated.sorted.reverse.map(print).sequence
      _         <- unpersist(train) >> unpersist(cv)
    } yield ()
  }
  def aucFunc(cv: TypedDataset[UserArtistData], bArtistIds: Broadcast[Seq[Int]])(m: ALSModel): F[Double] =
    F.ask.flatMap { spark =>
      import spark.implicits._
      def predictFunction(in: TypedDataset[UserArtist]) = {
        val df     = m.transform(in.toDF)
        val result = df.withColumn("prediction", df.col("prediction").cast(DoubleType)).as[UserArtistPrediction]
        TypedDataset.create(result)
      }
      areaUnderCurve(cv, bArtistIds, predictFunction)
    }
  def tryCombinations(ds: TypedDataset[UserArtistData])(aucFunc: ALSModel => F[Double])
  : List[F[(Double, (Int, Double, Double))]] = for {
//    rank     <- List(5, 30)
//    regParam <- List(1.0, 0.0001)
//    alpha    <- List(1.0, 40.0)
      rank     <- List(30)
      regParam <- List(1.0)
      alpha    <- List(40.0)
  } yield useALSModel(rank, regParam, alpha, ds, aucFunc)

  def useALSModel[O](rank: Int, regParam: Double, alpha: Double, train: TypedDataset[_], aucFunc: ALSModel => F[O])
  : F[(O, (Int, Double, Double))] = bracket {
    delay(buildALSModel(rank, regParam, alpha, train))
  } (aucFunc(_).map((_, (rank, regParam, alpha)))) { model =>
    unpersistF(model.userFactors) >> unpersistF(model.itemFactors) >> S.unit
  }
  def areaUnderCurve(
      userArtistData:  TypedDataset[UserArtistData],
      bAllArtistIDs:   Broadcast[Seq[Int]],
      predictFunction: TypedDataset[UserArtist] => TypedDataset[UserArtistPrediction]
  ): F[Double] = F.ask.flatMap { spark =>
    import spark.implicits._

    def join(p: TypedDataset[UserArtistPrediction], n: TypedDataset[UserArtistPrediction])
    : TypedDataset[(UserId, Option[Prediction], Option[Prediction])] = {
      val j = p.joinInner(n)( p('userId) === n('userId) )
      j.select(
        j.colMany('_1, 'userId),
        j.colMany('_1, 'prediction),
        j.colMany('_2, 'prediction)
      )
    }
    def allCounts(j: TypedDataset[(UserId, Option[Prediction], Option[Prediction])]): TypedDataset[(UserId, Int)] =
      j.groupBy(j('_1)).agg(count(j('_1)).cast[Int])

    def correctCounts(b: TypedDataset[(UserId, Option[Prediction], Option[Prediction])]): TypedDataset[(UserId, Int)] = b
        .filter(b('_2) > b('_3))
        .groupBy(b('_1))
        .agg(count(b('_1)).cast[Int])

    def meanAuc(all: TypedDataset[(UserId, Count)], correct: TypedDataset[(UserId, Count)]): F[Double] = {
      val j = all.joinLeft(correct)(all('_1) === correct('_1))
      val k = j.makeUDF((o: Option[(UserId, Count)]) => o.fold(0)(_._2))
      val r = j.select(j.colMany('_1, '_1), k(j('_2)) / j.colMany('_1, '_2))

      r.agg(avg(r('_2))).firstOption[F].map(_.getOrElse(0))
    }
    val positiveData = userArtistData.project[UserArtist]
    val negativeData = groupAndFlatMap(userArtistData)(_.userId) { case (userID, iter) =>
      val positives = iter.map(_.artistId).toSet
      chooseAtRandom(positives.size, bAllArtistIDs.value, positives).map(UserArtist(userID, _))
    }
    val positivePredictions = predictFunction(positiveData)
    val negativePredictions = predictFunction(negativeData)
    val bothPredictions     = join(positivePredictions, negativePredictions).cache()

    useCacheM(bothPredictions) { b =>
      meanAuc(allCounts(b), correctCounts(b))
    }
  }
  def predictMostListened(
    train: TypedDataset[UserArtistData])(
    all:   TypedDataset[UserArtist])
  : TypedDataset[UserArtistPrediction] = {
    val s = train.groupBy(train('artistId))
                 .agg(sum(train('playCount)).cast[Double])
                 .as[ArtistPrediction]
    val j = all.joinLeft(s)(all('artistId) === s('artistId))

    val countSum = j.makeUDF((_: Option[ArtistPrediction]).map(_.prediction))
    j.select(
      j.colMany('_1, 'userId),
      j.colMany('_1, 'artistId),
      countSum(j('_2))
    ).as[UserArtistPrediction]
  }
}
object EvaluateModelDemoMain extends Ch03Base with IOApp with UsesSparkSession[IO] {
  val S: Sync[IO] = implicitly[Sync[IO]]
  private val instance = new EvaluateDemo[Action] {
    val F: SparkAsk[Action] = implicitly[SparkAsk[Action]]
    val S: Sync[Action] = implicitly[Sync[Action]]
  }
  def run(args: List[String]): IO[ExitCode] =
    useSpark(program[Action](instance.evaluate).run) as ExitCode.Success
}

