package frameless_aas.ch03

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.mtl.ApplicativeAsk
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.instances.list._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.broadcast.Broadcast
import frameless.{TypedColumn, TypedDataset, TypedEncoder}
import frameless.cats.implicits._
import frameless_aas._
import frameless.functions.aggregate._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

trait EvaluateDemo[F[_]] {
  implicit val F: ApplicativeAsk[F, SparkSession]
  implicit val S: Sync[F]
  import S._

  private def print(s: Any) = S.delay(println(s))

  def broadcast[T: ClassTag](value: T): F[Broadcast[T]] =
    F.ask.map(_.sparkContext.broadcast(value))

  def evaluate(
    userArtists: TypedDataset[UserArtistData],
    artistData: TypedDataset[ArtistData],
    aliases: TypedDataset[ArtistAlias]
  ): F[Unit] = {
    def useALSModel[O](rank: Int, regParam: Double, alpha: Double, train: TypedDataset[_], aucFunc: ALSModel => F[O])
    : F[(O, (Int, Double, Double))] = bracket {
      delay(buildALSModel(rank, regParam, alpha, train))
    } (aucFunc(_).map((_, (rank, regParam, alpha)))) { model =>
      unpersistF(model.userFactors) >> unpersistF(model.itemFactors) >> S.unit
    }
    def tryCombinations(ds: TypedDataset[UserArtistData])(aucFunc: ALSModel => F[Double])
    : List[F[(Double, (Int, Double, Double))]] = for {
      rank     <- List(/*5, */30)
      regParam <- List(1.0/*, 0.0001*/)
      alpha    <- List(/*1.0, */40.0)
    } yield useALSModel(rank, regParam, alpha, ds, aucFunc)

    for {
      all              <- S.pure(canonicalize(userArtists, aliases))
      bAllArtistIDs    <- all.select(all('artistId)).distinct.collect() >>= broadcast

      Array(train, cv) =  all.randomSplit(Array(0.9, 0.1))
      _                <- cache(train) >> cache(cv)

      mostListenedAUC  <- areaUnderCurve(cv, bAllArtistIDs, predictMostListened(train))
      _                <- print(mostListenedAUC)

      f         =  (m: ALSModel) => areaUnderCurve(cv, bAllArtistIDs, conv(m.transform))
      evaluated <- tryCombinations(train)(f).toList.sequence
      _         <- evaluated.sorted.reverse.map(print).sequence
      _         <- unpersist(train) >> unpersist(cv)
    } yield ()
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

  def areaUnderCurve(
      positiveData: TypedDataset[UserArtistData],
      bAllArtistIDs: Broadcast[Seq[Int]],
      predictFunction: (TypedDataset[UserArtist] => TypedDataset[UserArtistPrediction])
  ): F[Double] = F.ask.flatMap { spark =>
    import spark.implicits._

    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive".
    // Make predictions for each of them, including a numeric score
    val positiveUA = positiveData.select(positiveData('userId), positiveData('artistId)).as[UserArtist]
    val positivePredictions =
      predictFunction(positiveUA)
//      .withColumnRenamed("prediction", "positivePrediction")

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other artists, excluding those that are "positive" for the user.
    val negativeData = positiveData
      .select(positiveData('userId), positiveData('artistId))
      .toDF
      .as[(Int,Int)]
      .groupByKey { case (user, _) => user }
      .flatMapGroups { case (userID, userIDAndPosArtistIDs) =>
        val random = new Random()
        val posItemIDSet = userIDAndPosArtistIDs.map { case (_, artist) => artist }.toSet
        val negative = new ArrayBuffer[Int]()
        val allArtistIDs = bAllArtistIDs.value
        var i = 0
        // Make at most one pass over all artists to avoid an infinite loop.
        // Also stop when number of negative equals positive set size
        while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
          val artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
          // Only add new distinct IDs
          if (!posItemIDSet.contains(artistID)) {
            negative += artistID
          }
          i += 1
        }
        // Return the set with user ID added back
        negative.map(artistID => (userID, artistID))
      }.toDF("userId", "artistId")
      .as[UserArtist]
      .typed

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeData)
//                              . withColumnRenamed("prediction", "negativePrediction")

    // Join positive predictions to negative predictions by user, only.
    // This will result in a row for every possible pairing of positive and negative
    // predictions within each user.
    val temp = positivePredictions.joinInner(negativePredictions)(
      positivePredictions('userId) === negativePredictions('userId)
    )
    val joinedPredictions: TypedDataset[(Int, Double, Double)] = temp.select(
      temp.colMany('_1, 'userId),
      temp.colMany('_1, 'prediction),
      temp.colMany('_2, 'prediction)
    ).cache()

    // Count the number of pairs per user
    import frameless.functions._
    import frameless.functions.aggregate._

    val allCountsTmp0 = joinedPredictions.withColumn[(Int, Double, Double, Int)](lit(1))
    val allCountsTmp = allCountsTmp0.select(allCountsTmp0('_1), allCountsTmp0('_4))
    val allCounts: TypedDataset[(Int, Int)] = allCountsTmp
      .groupBy(allCountsTmp('_1)) // "userId")
      .agg(count(allCountsTmp('_2)).cast[Int]) //.agg(count(lit("1")).as("total"))
    val correctCounts: TypedDataset[(Int, Int)] = joinedPredictions
      .filter(joinedPredictions('_2) > joinedPredictions('_3))
      .groupBy(joinedPredictions('_1))
      .agg(count(joinedPredictions('_1)).cast[Int])
//      .select("userId", "correct")

    // Combine these, compute their ratio, and average over all users
    val ds0 = allCounts.joinLeft(correctCounts)(allCounts('_1) === correctCounts('_1))
    val f: Option[(Int, Int)] => Option[Int] = o => o.map(_._2)
    val g: TypedColumn[((Int, Int), Option[(Int, Int)]), Option[(Int, Int)]] => TypedColumn[((Int, Int), Option[(Int, Int)]), Option[Int]] = ds0.makeUDF(f)
    val meanAUCJoin: TypedDataset[(Int, Int, Int)] = ds0.select(ds0.colMany('_1, '_1), ds0.colMany('_1, '_2), g(ds0('_2)).getOrElse(0))
    val meanAUC1 = meanAUCJoin.select(meanAUCJoin('_1), meanAUCJoin('_3) / meanAUCJoin('_2))
    val ds2: TypedDataset[Double] = meanAUC1.agg(avg(meanAUC1('_2)))
    val meanAUC: F[Double] = ds2.firstOption[F].map(_.getOrElse(0))

    joinedPredictions.unpersist()

    meanAUC
  }

  def predictMostListened(train: TypedDataset[UserArtistData])(allData: TypedDataset[UserArtist]): TypedDataset[UserArtistPrediction] = {
    val table: TypedDataset[(Int, Double)] = train
      .groupBy(train('artistId))
      .agg(sum(train.apply[Int]('playCount)).cast[Double])
    val joined: TypedDataset[(UserArtist, Option[(Int, Double)])] = allData.joinLeft(table)(allData('artistId) === table('_1))
    val extractGoodId = joined.makeUDF((_: Option[(Int, Double)]).map(_._2))
    joined.select(joined.colMany('_1, 'userId), joined.colMany('_1, 'artistId), extractGoodId(joined('_2)).getOrElse(0.0)).as[UserArtistPrediction]
  }
  def selectArtists(t: TypedDataset[UserArtistData], userId: Int): TypedDataset[Int] =
    t.filter(t('userId) === userId)
     .select(t('artistId))
     .as[Int]
}
object EvaluateModelDemoMain extends Ch03Base with IOApp with UsesSparkSession[IO] {
  val S: Sync[IO] = implicitly[Sync[IO]]
  private val instance = new EvaluateDemo[Action] {
    val F: ApplicativeAsk[Action, SparkSession] = implicitly[ApplicativeAsk[Action, SparkSession]]
    val S: Sync[Action] = implicitly[Sync[Action]]
  }
  def run(args: List[String]): IO[ExitCode] =
    useSpark(program[Action](instance.evaluate).run) as ExitCode.Success
}











