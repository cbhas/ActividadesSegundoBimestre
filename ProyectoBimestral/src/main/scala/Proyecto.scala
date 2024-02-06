import com.github.tototoshi.csv.*
import java.io.File
import org.nspl.*
import org.nspl.awtrenderer.*


implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object Exporter {
  @main
  def generateStatistics(): Unit =
    val path2DataFile1: String = "excel's\\tablaPartidosYGoles.csv"
    val path2DataFile2: String = "excel's\\tablaAlineacionesXTorneo.csv"

    val reader1: CSVReader = CSVReader.open(new File(path2DataFile1))
    val reader2: CSVReader = CSVReader.open(new File(path2DataFile2))

    val contentFile1: List[Map[String, String]] = reader1.allWithHeaders()
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()

    reader1.close()
    reader2.close()

    calculateGoalsStats(contentFile1)
    playerPositionFrequency(contentFile2)
    ownGoalsStats(contentFile1)

    // Mínimo, Promedio y Máximo de goles por partido:
    def calculateGoalsStats(matches: List[Map[String, String]]): Unit = {
      val goalsPerMatch: List[Int] = matches.map(s => s("matches_home_team_score").toInt + s("matches_away_team_score").toInt)
      val minGoals: Int = goalsPerMatch.min
      val avgGoals: Double = goalsPerMatch.sum.toDouble / goalsPerMatch.length
      val maxGoals: Int = goalsPerMatch.max

      println(s"Mínimo de goles por partido: $minGoals")
      println(s"Promedio de goles por partido: $avgGoals")
      println(s"Máximo de goles por partido: $maxGoals \n")
    }

    // Frecuencia de posiciones de los jugadores:
    def playerPositionFrequency(alignments: List[Map[String, String]]): Unit = {
      val positions: List[String] = alignments.map(align => align("squads_position_name"))
      val positionCounts: Map[String, Int] = positions.groupBy(identity).map(entry => (entry._1, entry._2.size))

      positionCounts.foreach { case (position, count) =>
        println(s"Frecuencia de $position: $count")
      }
      println()
    }

    // Estadísticas sobre goles en propia:
    def ownGoalsStats(goals: List[Map[String, String]]): Unit = {
      val ownGoals: Int = goals.count(goal => goal("goals_own_goal") == "1")
      val totalGoals: Int = goals.length
      val ownGoalsPercentage: Double = (ownGoals.toDouble / totalGoals) * 100

      println(s"Número de goles propios: $ownGoals")
      println(s"Porcentaje de goles propios: $ownGoalsPercentage%")
    }

}