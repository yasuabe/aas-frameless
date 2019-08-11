package frameless_aas.ch03

import cats.effect.Sync
import cats.syntax.functor._
import frameless.TypedDataset
import frameless.cats.implicits._

import scala.util.Try

case class UserArtistData(userId: Int, artistId: Int, playCount: Int)
object UserArtistData {
  def apply(line: String) = {
    val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
    new UserArtistData(userID, artistID, count)
  }
}
case class ArtistData(id: Int, name: String)
object ArtistData {
  def apply(line: String): Option[ArtistData] = line.span(_ != '\t') match {
    case (_,  "") => None
    case (id, s)  => Try(ArtistData(id.toInt, s.trim)).toOption
  }
}
case class ArtistAlias(badId: Int, goodId: Int)
object ArtistAlias {
  def apply(line: String): Option[ArtistAlias] = line.span(_ != '\t') match {
    case ("", _)     => None
    case (bad, good) => (bad != good).toOption(ArtistAlias(bad.toInt, good.trim.toInt))
  }
  def canonicalMap[F[_]: Sync](d: TypedDataset[ArtistAlias]): F[Map[Int, Int]] =
    d.collect[F].map { s: Seq[ArtistAlias] =>
      val m = s.filterNot(x => x.goodId == x.badId).map(a => a.badId -> a.goodId).toMap
      m map { case (k, v) => k -> m.getOrElse(v, v) }
    }
}
case class UserArtist(userId: Int, artistId: Int)
case class ArtistPrediction(artistId: Int, prediction: Double)
case class ArtistPrediction2(artistId: Int, prediction: Int)
case class UserArtistPrediction(userId: Int, artistId: Int, prediction: Option[Double])
