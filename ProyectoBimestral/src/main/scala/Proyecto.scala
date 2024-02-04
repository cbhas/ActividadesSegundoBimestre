import com.github.tototoshi.csv.*
import java.io.File

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object Exporter
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

// Sebastián "cbhas" Calderón