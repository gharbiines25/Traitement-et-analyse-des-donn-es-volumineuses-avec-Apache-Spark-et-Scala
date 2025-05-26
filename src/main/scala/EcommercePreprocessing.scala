import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.lang3.StringUtils
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class EcommerceSession(
                             user_id: String,
                             session_duration: Int,
                             pages_viewed: Int,
                             product_category: String,
                             purchase_amount: Double,
                             review_score: Double,
                             review_comment: String,
                             timestamp: String,
                             device_type: String,
                             country: String,
                             city: String
                           )

object EcommercePreprocessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ecommerce Data Preprocessing").setMaster("local[*]")
    val sc = new SparkContext(conf)

    println("🟡 Chargement du fichier CSV...")
    val rawData = sc.textFile("data/ecommerce_data_enriched.csv")
    println(s"📄 Nombre total de lignes brutes : ${rawData.count()}")

    println("🟡 Suppression de l'en-tête...")
    val header = rawData.first()
    val noHeader = rawData.filter(_ != header)
    println(s"📄 Nombre de lignes après suppression de l'en-tête : ${noHeader.count()}")

    println("🟡 Filtrage des lignes mal formées (11 colonnes)...")
    val cleaned = noHeader.filter(_.split(",", -1).length == 11)
    println(s"📄 Nombre de lignes bien formées : ${cleaned.count()}")

    println("🟡 Suppression des valeurs nulles ou vides...")
    val cleanedNoNulls = cleaned.filter { line =>
      val cols = line.split(",", -1).map(_.trim)
      !cols.contains("") && !cols.contains("null")
    }
    println(s"📄 Nombre de lignes sans valeurs nulles : ${cleanedNoNulls.count()}")

    println("🟡 Filtrage des lignes avec données aberrantes...")
    val validData = cleanedNoNulls.filter { line =>
      val cols = line.split(",", -1).map(_.trim)
      try {
        val duration = cols(1).toInt
        val pages = cols(2).toInt
        val amount = cols(4).toDouble
        val score = cols(5).toDouble
        duration >= 0 && pages >= 0 && amount >= 0 && score >= 1 && score <= 5
      } catch {
        case _: Exception => false
      }
    }
    println(s"📄 Nombre de lignes valides après filtrage : ${validData.count()}")

    println("🟡 Transformation des lignes en objets typés (case class)...")
    val sessions = validData.map { line =>
      val cols = line.split(",", -1).map(_.trim)
      val duration = math.min(cols(1).toInt, 300)
      val pages = math.min(cols(2).toInt, 100)
      val amount = math.min(cols(4).toDouble, 1000.0)

      EcommerceSession(
        cols(0),
        duration,
        pages,
        StringUtils.capitalize(cols(3).toLowerCase),
        amount,
        cols(5).toDouble,
        cols(6),
        cols(7),
        cols(8),
        cols(9),
        StringUtils.stripAccents(cols(10))
      )
    }.cache()
    println(s"✅ Sessions valides mises en cache : ${sessions.count()}")

    println("📊 Analyse : sessions par heure de la journée...")
    val sessionsByHour = sessions
      .map { s =>
        val hour = try {
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
          LocalDateTime.parse(s.timestamp, formatter).getHour
        } catch {
          case _: Exception => 0
        }
        (hour, 1)
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()

    println("📊 Analyse : montant d'achats par heure de la journée...")
    val purchasesByHour = sessions
      .map { s =>
        val hour = try {
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
          LocalDateTime.parse(s.timestamp, formatter).getHour
        } catch {
          case _: Exception => 0
        }
        (hour, s.purchase_amount)
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()

    println("📊 Analyse : distribution des notes...")
    val reviewDist = sessions
      .map(s => (s.review_score.toString, 1))
      .reduceByKey(_ + _)
      .collect()
      .sortBy(_._1.toDouble)

    println("📊 Analyse : taux de conversion par type d'appareil...")
    val conversionByDevice = sessions
      .map(s => (s.device_type, if (s.purchase_amount > 0) 1 else 0))
      .aggregateByKey((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (a, b) => (a._1 + b._1, a._2 + b._2)
      )
      .mapValues { case (converted, total) => (converted.toDouble / total) * 100 }
      .collect()

    def stringify(seq: Seq[String]) = seq.map(_.replace("\"", "\\\"")).mkString("\"", "\",\"", "\"")
    def toStr[T](seq: Seq[T]) = seq.mkString(",")

    println("📄 Génération du fichier HTML avec Chart.js...")
    val html = s"""
                  |<html>
                  |<head>
                  |<script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>
                  |<style>
                  |  body { font-family: Arial; margin: 40px; }
                  |  h2 { margin-top: 40px; font-size: 20px; }
                  |  canvas {
                  |    margin-bottom: 40px;
                  |    max-width: 600px;
                  |    max-height: 300px;
                  |  }
                  |</style>
                  |</head>
                  |<body>
                  |<h1>Tableau de bord Analyse E-Commerce</h1>
                  |
                  |<h2>Sessions par heure de la journée</h2>
                  |<canvas id=\"chart6\"></canvas>
                  |
                  |<h2>Montant d'achats par heure de la journée</h2>
                  |<canvas id=\"chart7\"></canvas>
                  |
                  |<h2>Distribution des notes d'avis</h2>
                  |<canvas id=\"chart8\"></canvas>
                  |
                  |<h2>Taux de conversion par type d'appareil</h2>
                  |<canvas id=\"chart9\"></canvas>
                  |
                  |<script>
                  |new Chart(document.getElementById('chart6'), {
                  |  type: 'line',
                  |  data: {
                  |    labels: [${toStr(sessionsByHour.map(_._1.toString))}],
                  |    datasets: [{
                  |      label: 'Nombre de sessions',
                  |      data: [${toStr(sessionsByHour.map(_._2))}],
                  |      fill: false,
                  |      borderColor: 'rgba(255,99,132,1)',
                  |      tension: 0.1
                  |    }]
                  |  }
                  |});
                  |
                  |new Chart(document.getElementById('chart7'), {
                  |  type: 'line',
                  |  data: {
                  |    labels: [${toStr(purchasesByHour.map(_._1.toString))}],
                  |    datasets: [{
                  |      label: 'Total des achats (€)',
                  |      data: [${toStr(purchasesByHour.map(_._2))}],
                  |      fill: true,
                  |      backgroundColor: 'rgba(54, 162, 235, 0.2)',
                  |      borderColor: 'rgba(54, 162, 235, 1)',
                  |      tension: 0.4
                  |    }]
                  |  }
                  |});
                  |
                  |new Chart(document.getElementById('chart8'), {
                  |  type: 'pie',
                  |  data: {
                  |    labels: [${stringify(reviewDist.map(_._1))}],
                  |    datasets: [{
                  |      label: 'Distribution des notes',
                  |      data: [${toStr(reviewDist.map(_._2))}],
                  |      backgroundColor: [
                  |        'rgba(255, 99, 132, 0.6)',
                  |        'rgba(255, 159, 64, 0.6)',
                  |        'rgba(255, 205, 86, 0.6)',
                  |        'rgba(75, 192, 192, 0.6)',
                  |        'rgba(54, 162, 235, 0.6)'
                  |      ]
                  |    }]
                  |  }
                  |});
                  |
                  |new Chart(document.getElementById('chart9'), {
                  |  type: 'bar',
                  |  data: {
                  |    labels: [${stringify(conversionByDevice.map(_._1))}],
                  |    datasets: [{
                  |      label: 'Taux de conversion (%)',
                  |      data: [${toStr(conversionByDevice.map(_._2))}],
                  |      backgroundColor: 'rgba(255, 159, 64, 0.6)',
                  |      borderColor: 'rgba(255, 159, 64, 1)',
                  |      borderWidth: 1
                  |    }]
                  |  }
                  |});
                  |</script>
                  |</body>
                  |</html>
                  |""".stripMargin


    Files.createDirectories(Paths.get("output"))
    Files.write(Paths.get("output/dashboard.html"), html.getBytes, StandardOpenOption.CREATE)

    println("✅ Dashboard HTML généré avec succès : output/dashboard.html")

    sc.stop()
    println("🛑 SparkContext arrêté.")
  }
}
