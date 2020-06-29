import java.util.Date

import scala.io.Source
import play.api.libs.json._

object Testing {
  def main(args: Array[String]): Unit = {
    println()
    val lines = Source.fromFile("/home/javi/Desktop/Workspaces/Java/consumer/src/main/scala/text.txt").getLines.toList
    val line: String = lines(0)

    val json: JsValue = Json.parse(line)
    val titles = (json \\ "title").map(_.as[String])
    val descriptions = (json \\ "description").map(_.as[String])
    println("Title")
    println(titles(1))
    println("Descrption")
    println(descriptions(1))
    println("+++++++ Result ++++++++++++")
    val values = titles.zip(descriptions)
    println(values.length)
    println(values(0))
    println("==============")
    println(values)

//    println(Some(values))
//    println(descriptions(0))
//    val he:Seq[Int] = Seq(1,2,3)
//    val holaa = he.map(_ * 2)
//    println(holaa)
  }
}
