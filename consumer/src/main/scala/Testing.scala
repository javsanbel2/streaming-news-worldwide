import java.util.Date

import scala.io.Source
import play.api.libs.json._

object Testing {
  def main(args: Array[String]): Unit = {
    val h = Array("z", "y")
    val a = Array("1", "2")
    val b = Array("a", "b")

    println((h,a,b).zipped.toList)

//    println(Some(values))
//    println(descriptions(0))
//    val he:Seq[Int] = Seq(1,2,3)
//    val holaa = he.map(_ * 2)
//    println(holaa)
  }
}
