import cats.*
import cats.implicits.*
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}

import java.io.File

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object Exporter
  @main
  def exportFunc(): Unit =
    val path2DataFile1: String = "excel's\\tablaPartidosYGoles.csv"
    val path2DataFile2: String = "excel's\\tablaAlineacionesXTorneo.csv"

    val reader1: CSVReader = CSVReader.open(new File(path2DataFile1))
    val reader2: CSVReader = CSVReader.open(new File(path2DataFile2))
    
    val contentFile1: List[Map[String, String]] = reader1.allWithHeaders()
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()

    reader1.close()
    reader2.close()

    // generateGoals(contentFile1)
    // generateMatches(contentFile1)
    generatePlayers(contentFile2)
    // generateTeams(contentFile1)

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

  // Porque todos los equipos jugaron de local o de visita alguna vez
  private def generateTeams(data: List[Map[String, String]]): Unit =
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

  private def generateGoals(data: List[Map[String, String]]): Unit =
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

  private def generateMatches(data: List[Map[String, String]]): Unit =
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

  private def generatePlayers(data: List[Map[String, String]]): Unit =
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
    println(players.size)

// Carlos "cdm18" Mejía & Sebastián "cbhas" Calderón
