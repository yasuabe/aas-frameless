import cats.effect.Sync
import cats.syntax.option._
import cats.mtl.ApplicativeAsk
import frameless.{Injection, TypedDataset, TypedEncoder}
import frameless.syntax._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import cats.syntax.flatMap._
import cats.syntax.functor._
import frameless_aas.ch03.SparkAsk
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

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

  def cache[F[_]: Sync, T](t: TypedDataset[T]): F[TypedDataset[T]] = Sync[F].delay(t.cache())
  def unpersist[F[_]: Sync, T](t: TypedDataset[T]): F[TypedDataset[T]] = Sync[F].delay(t.unpersist())
  def unpersist_[F[_], T](t: TypedDataset[T])(implicit S: Sync[F]): F[Unit] = unpersist(t) >> S.unit
  def unpersistF[F[_]: Sync, T](ds: Dataset[T]): F[Dataset[T]] = Sync[F].delay(ds.unpersist())

  def groupAndFlatMap[I, K: Encoder, O: Encoder: TypedEncoder](
    t: TypedDataset[I])(
    k: I => K)(
    f: (K, Iterator[I]) =>  TraversableOnce[O])
  : TypedDataset[O] = t.dataset.groupByKey(k).flatMapGroups(f).as[O].typed

  def chooseAtRandom[T](n: Int, source: Seq[T], excludes: Set[T]): TraversableOnce[T] = {
    val random = new Random()
    val result = new ArrayBuffer[T]()
    var i = 0
    while (i < source.length && result.size < n) {
      val elem = source(random.nextInt(source.length))
      if (!excludes.contains(elem)) result += elem
      i += 1
    }
    result
  }
  def useCache[F[_], T, R](t: TypedDataset[T])(f: TypedDataset[T] => R)(implicit S: Sync[F]): F[R] =
    S.bracket(cache(t))(t => S.delay(f(t)))(unpersist_(_))

  def useCacheM[F[_], T, O](a: TypedDataset[T])(f: TypedDataset[T] => F[O])(implicit S: Sync[F]): F[O] =
    S.bracket(cache(a))(f)(unpersist_(_))

  implicit class BooleanOps(val b: Boolean) extends AnyVal {
    def toOption[T](v: => T): Option[T] =
      if (b) Some(v) else None
  }
  def broadcast[F[_]: Sync, T: ClassTag](value: T)(implicit F: SparkAsk[F]): F[Broadcast[T]] =
    F.ask.map(_.sparkContext.broadcast(value))

  implicit val i: Injection[Option[Double], Double] = new Injection[Option[Double], Double] {
    def apply(a: Option[Double]): Double = a.getOrElse(Double.NaN)
    def invert(b: Double): Option[Double] = b.some
  }
}
