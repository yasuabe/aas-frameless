package frameless_aas

import cats.Applicative
import cats.data.ReaderT
import cats.effect.{IO, Sync}
import cats.mtl.ApplicativeAsk
import cats.mtl.instances.local.askReader
import cats.syntax.flatMap._
import cats.syntax.functor._
import frameless.syntax._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.TypeTag

package object ch03 {
  implicit class BoolToOption(val self: Boolean) extends AnyVal {
    def toOption[A](value: => A): Option[A] =
      if (self) Some(value) else None
  }

  type Action[T] = ReaderT[IO, SparkSession, T]
  implicit val readerIOApplicativeAsk: ApplicativeAsk[Action, SparkSession] =
    askReader[IO, SparkSession]

  def resource[F[_], T, R](ds: TypedDataset[T])(f: TypedDataset[T] => R)(implicit S: Sync[F]): F[R] = {
    S.bracket(
      S.delay(ds.cache())
    )(
      ds => S.delay(f(ds))
    )(
      ds => S.delay(ds.unpersist()).as(())
    )
  }
  def readLines[F[_]: Applicative, T <: Product: TypeTag: TypedEncoder](
    fileName: String, f: String => T)(implicit F: ApplicativeAsk[F, SparkSession]
  ): F[TypedDataset[T]] = {
    def read(spark: SparkSession) = {
      import spark.implicits.newProductEncoder
      spark.read.textFile(fileName).map(f)
    }
    F.ask.map(read(_).typed)
  }

  def flatReadLines[F[_]: Applicative, T <: Product: TypeTag](
    fileName: String,
    f: String => TraversableOnce[T]
  )(implicit F: ApplicativeAsk[F, SparkSession], te: TypedEncoder[T]
  ): F[TypedDataset[T]] = {
    def read(spark: SparkSession) = {
      import spark.implicits.newProductEncoder
      spark.read.textFile(fileName).flatMap(f)
    }
    F.ask.map(spark => read(spark).typed)
  }
  trait Ch03Base {
    type Func[F[_]] = (TypedDataset[UserArtistData], TypedDataset[ArtistData], TypedDataset[ArtistAlias]) => F[Unit]
    val path = "files/profiledata_06-May-2005"
    def program[F[_]: Applicative](f: Func[F])(implicit F: ApplicativeAsk[F, SparkSession], S: Sync[F]): F[Unit] = for {
      playData <- readLines(s"$path/user_artist_data.txt", UserArtistData(_))
      artists  <- flatReadLines(s"$path/artist_data.txt", ArtistData(_))
      aliases  <- flatReadLines(s"$path/artist_alias.txt", ArtistAlias(_))
      _        <- f(playData, artists, aliases)
    } yield ()
  }
}
