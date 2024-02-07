import com.github.tototoshi.csv.*
import org.nspl.*
import org.nspl.awtrenderer.*
import org.nspl.data.HistogramData
import java.io.File
import doobie.implicits.*
import cats.*
import cats.effect.*
import doobie.Transactor
import scala.collection.immutable.List
import cats.effect.unsafe.implicits.global

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object proyectoIntegrador {
  @main
  def generateStatistics(): Unit =
    val path1DataFile1 = "excel's\\tablaPartidosYGoles.csv"
    val path1DataFile2 = "excel's\\tablaAlineacionesXTorneo.csv"

    val reader1 = CSVReader.open(new File(path1DataFile1))
    val reader2 = CSVReader.open(new File(path1DataFile2))

    val contentFile1 = reader1.allWithHeaders()
    val contentFile2 = reader2.allWithHeaders()

    reader1.close()
    reader2.close()

    calculateGoalsStats(contentFile1)
    playerPositionFrequency(contentFile2)
    ownGoalsStats(contentFile1)
    winnerEcuadorVsBrazil(contentFile1)
    firstLatamCountryWin(contentFile1)

    def calculateGoalsStats(matches: List[Map[String, String]]): Unit =
      val goalsPerMatch: List[Int] = matches.map(s => s("matches_home_team_score").toInt + s("matches_away_team_score").toInt)
      val minGoals: Int = goalsPerMatch.min
      val avgGoals: Double = goalsPerMatch.sum.toDouble / goalsPerMatch.length
      val maxGoals: Int = goalsPerMatch.max

      println(s"Mínimo de goles por partido: $minGoals")
      println(s"Promedio de goles por partido: $avgGoals")
      println(s"Máximo de goles por partido: $maxGoals \n")


    def playerPositionFrequency(alignments: List[Map[String, String]]): Unit =
      val positions: List[String] = alignments.map(row => row("squads_position_name"))
      val positionCounts: Map[String, Int] = positions.groupBy(identity).map(t => (t._1, t._2.size))

      positionCounts.foreach((position, count) => println(s"Frecuencia de $position: $count"))
      println()


    def ownGoalsStats(goals: List[Map[String, String]]): Unit =
      val ownGoals: Int = goals.count(goal => goal("goals_own_goal") == "1")
      val totalGoals: Int = goals.length
      val ownGoalsPercentage: Double = (ownGoals.toDouble / totalGoals) * 100

      println(s"Número de goles marcados en propia: $ownGoals")
      println(s"Porcentaje de goles en propia: $ownGoalsPercentage%")
      println()


    def winnerEcuadorVsBrazil(data: List[Map[String, String]]): Unit =
      val winnings = data
        .filter(row =>
          (row("away_team_name") == "Ecuador" || row("home_team_name") == "Ecuador") &&
            (row("away_team_name") == "Brazil" || row("home_team_name") == "Brazil"))
        .count(row =>
          row("home_team_win") == "Ecuador" || row("away_team_win") == "Ecuador")
      println(s"Ecuador le ha ganado $winnings veces a Brazil en un mundial")


    def firstLatamCountryWin(data: List[Map[String, String]]): Unit =
      val countriesLatamWorldCup = data
        .filter(row => row("tournaments_tournament_name").contains("Men's"))
        .filter(row => row("away_region_name") == "South America")
        .map(row => row("away_team_name")).distinct

      val winners = data
        .filter(row => row("tournaments_tournament_name").contains("Men's"))
        .map(row => (row("tournaments_winner"), row("matches_tournament_id"))).distinct

      val primerCampeonSudamericano = winners
        .filter((pais, id) => countriesLatamWorldCup.contains(pais)).head._1
      print(s"El primer país Latinoamericano en ganar un mundial, fué: ${primerCampeonSudamericano}")s


  @main
  def generateScripts(): Unit =
    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/practicum",
      user = "root",
      password = "cbhas123",
      logHandler = None
    )

    val path2DataFile1 = "excel's\\tablaPartidosYGoles.csv"
    val path2DataFile2 = "excel's\\tablaAlineacionesXTorneo.csv"

    val reader1 = CSVReader.open(new File(path2DataFile1))
    val reader2 = CSVReader.open(new File(path2DataFile2))

    val contentFile1 = reader1.allWithHeaders()
    val contentFile2 = reader2.allWithHeaders()

    reader1.close()
    reader2.close()

    generateGoals(contentFile1)
    generateMatches(contentFile1)
    generatePlayers(contentFile2)
    generateSquads(contentFile2).foreach(
      insert => insert.run.transact(xa).unsafeRunSync())
    generateStadiums(contentFile1).foreach(
      insert => insert.run.transact(xa).unsafeRunSync())
    generateTeams(contentFile1)
    generateTournaments(contentFile1).foreach(
      insert => insert.run.transact(xa).unsafeRunSync())

    def defaultValue(text: String): Double =
      if(text.equals("NA"))
        0
      else
        text.toDouble

    def notAvailable(text: String): String =
      if(text.equals("not available"))
        "1970-1-1"
      else
        text

    def generateGoals(data: List[Map[String, String]]): Unit =
      val sqlInsert: String = s"INSERT INTO goals VALUES('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', '%s', %d);"
      val goals: List[String] = data
        .map(
          row => (
            row("goals_goal_id"),
            row("matches_match_id"),
            row("goals_team_id"),
            row("goals_player_id"),
            row("goals_player_team_id"),
            row("goals_minute_label").replaceAll("'", "\\\\'"),
            row("goals_minute_regulation").toInt,
            row("goals_minute_stoppage").toInt,
            row("goals_match_period"),
            row("goals_own_goal"),
            row("goals_penalty").toInt
            )
        )
        .distinct
        .sortBy(_._1)
        .map(t2 => sqlInsert.format(t2._1, t2._2, t2._3, t2._4, t2._5, t2._6, t2._7, t2._8, t2._9, t2._10, t2._11))

      goals.foreach(println)

    def generateMatches(data: List[Map[String, String]]): Unit =
      val sqlInsert: String = s"INSERT INTO matches VALUES('%s', '%s', '%s', '%s', '%s', %d, %d, '%s');"
      val matches: List[String] = data
        .map(
          row => (
            row("matches_match_id"),
            row("matches_tournament_id"),
            row("matches_stadium_id"),
            row("matches_home_team_id"),
            row("matches_away_team_id"),
            row("matches_home_team_score_penalties").toInt,
            row("matches_away_team_score_penalties").toInt,
            row("matches_result"))
        )
        .distinct
        .sortBy(_._1)
        .map(t3 => sqlInsert.format(t3._1, t3._2, t3._3, t3._4, t3._5, t3._6, t3._7, t3._8))

      matches.foreach(println)

    def generatePlayers(data: List[Map[String, String]]): Unit =
      val sqlInsert: String = s"INSERT INTO players VALUES('%s', '%s', '%s', '%s', %d, %d, %d, %d, %d);"
      val players: List[String] = data
        .map(
          row => (
            row("squads_player_id"),
            row("players_family_name").replaceAll("'", "\\\\'"),
            row("players_given_name").replaceAll("'", "\\\\'"),
            notAvailable(row("players_birth_date")),
            row("players_female").toInt,
            row("players_goal_keeper").toInt,
            row("players_defender").toInt,
            row("players_midfielder").toInt,
            row("players_forward").toInt)
        )
        .distinct
        .sortBy(_._1)
        .map(t3 => sqlInsert.format(t3._1, t3._2, t3._3, t3._4, t3._5, t3._6, t3._7, t3._8, t3._9))

      players.foreach(println)

    def generateSquads(data: List[Map[String, String]]) =
      val squadsTuple = data
        .map(
          row => (row("squads_player_id"),
            row("squads_tournament_id"),
            row("squads_shirt_number").toInt,
            row("squads_position_name"),
            row("squads_team_id"))
        )
        .map(t7 => sql"INSERT INTO squads VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5})".update)

      squadsTuple

    def generateStadiums(data: List[Map[String, String]]) =
      val stadiumsTuple = data
        .map(
          row => (row("matches_stadium_id"),
            row("stadiums_stadium_name"),
            row("stadiums_city_name"),
            row("stadiums_country_name"),
            row("stadiums_stadium_capacity").toInt)
        ).distinct
        .map(t7 => sql"INSERT INTO stadiums VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5})".update)

      stadiumsTuple

    def generateTeams(data: List[Map[String, String]]): Unit =
      val sqlInsert: String = s"INSERT INTO teams VALUES('%s', '%s', %d, %d, '%s');"
      val teams: List[String] = data
        .map(
          row => (
            row("matches_away_team_id"),
            row("away_team_name"),
            row("away_mens_team").toInt,
            row("away_womens_team").toInt,
            row("away_region_name"))
        )
        .distinct
        .sortBy(_._1)
        .map(t1 => sqlInsert.format(t1._1, t1._2, t1._3, t1._4, t1._5))

      teams.foreach(println)

    def generateTournaments(data: List[Map[String, String]]) =
      val tournamentsTuple = data
        .map(
          row => (row("matches_tournament_id"),
            row("tournaments_tournament_name"),
            row("tournaments_year").toInt,
            row("tournaments_host_country"),
            row("tournaments_winner"),
            row("tournaments_count_teams").toInt)
        ).distinct
        .map(t7 => sql"INSERT INTO tournaments VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5}, ${t7._6})".update)

      tournamentsTuple


  @main
  def generateGraphics(): Unit =
    val path3DataFile1 = "excel's\\tablaPartidosYGoles.csv"
    val path3DataFile2 = "excel's\\tablaAlineacionesXTorneo.csv"

    val reader1 = CSVReader.open(new File(path3DataFile1))
    val reader2 = CSVReader.open(new File(path3DataFile2))

    val contentFile1 = reader1.allWithHeaders()
    val contentFile2 = reader2.allWithHeaders()

    reader1.close()
    reader2.close()

    shirtDefenders(contentFile2)
    goalsMinute(contentFile1)
    stadiumCapacity(contentFile1)

    def shirtDefenders(data: List[Map[String, String]]): Unit =
      val shirtDefenders: List[Double] = data
          .filter(row => row("squads_position_name") == "defender" && row("squads_shirt_number") != "0")
          .map(row => row("squads_shirt_number").toDouble)

      val frequencyHistogram = xyplot(HistogramData(shirtDefenders, 10) -> point())(
        par
          .xlab("Numero Camiseta")
          .ylab("Cantidad")
          .main("Numero de Camiseta de los Defensas")
      )

      pngToFile(new File("graficas\\numero-camisetas-defensas.png"), frequencyHistogram.build, 1000)
      renderToByteArray(frequencyHistogram.build, width = 2000)
      println("Imagen (numero-camisetas-defensas.png) creada con éxito!!")

    def goalsMinute(data: List[Map[String, String]]): Unit =
      val goalsMinute: List[Double] = data
        .filter(row => row("goals_minute_regulation") != "NA")
        .map(row => row("goals_minute_regulation").toDouble)

      val frequencyHistogram = xyplot(HistogramData(goalsMinute, 20) -> bar())(
        par
          .xlab("Minutos")
          .ylab("Goles")
          .main("Goles por Minuto"),
      )

      pngToFile(new File("graficas/goles-minuto.png"), frequencyHistogram.build, 1000)
      renderToByteArray(frequencyHistogram.build, width = 2000)
      println("Imagen (graficas/goles-minuto.png) creada con éxito!!")

    def stadiumCapacity(data: List[Map[String, String]]): Unit =
      val stadiumCapacity: List[Double] = data
        .filter(row => row("stadiums_stadium_capacity") != "NA")
        .map(row => row("stadiums_stadium_capacity").toDouble)

      val frequencyHistogram = xyplot(HistogramData(stadiumCapacity, 10) -> line())(
        par
          .xlab("Capacidad de Estadios")
          .ylab("Cantidad")
          .main("Capacidad de Estadios")
      )

      pngToFile(new File("graficas\\capacidad_estadios.png"), frequencyHistogram.build, 1000)
      renderToByteArray(frequencyHistogram.build, width = 2000)
      println("Imagen (capacidad_estadios.png) creada con éxito!!")

}

// Carlos "cdm18" Mejía & Sebastián "cbhas" Calderón
