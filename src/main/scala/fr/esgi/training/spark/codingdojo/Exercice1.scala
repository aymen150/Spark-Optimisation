package fr.esgi.training.spark.codingdojo

import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import fr.esgi.training.spark.codingdojo.configuration.{CodingDojoConfiguration, Constants}
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

//In Eclipse go to Run > Run Configurations... > Arguments > VM arguments and set max heapsize like -Xmx512m.
object Exercice1 {

  case class Enregistrement(COMMUNE: String, ARM: String, IRIS: String, ACHL: String, AEMM: String, AEMMR: String, AGEMEN8: String, ANEM: String, ANEMR: String, ASCEN: String, BAIN: String, BATI: String, CATIRIS: String, CATL: String, CHAU: String, CHFL: String, CHOS: String, CLIM: String, CMBL: String, CUIS: String, DEROU: String, DIPLM_15: String, EAU: String, EGOUL: String, ELEC: String, EMPLM: String, GARL: String, HLML: String, ILETUDM: String, ILTM: String, IMMIM: String, INAIM: String, INEEM: String, INP11M: String, INP16M: String, INP18M: String, INP19M: String, INP24M: String, INP3M: String, INP60M: String, INP65M: String, INP6M: String, INP75M: String, INPAM: String, INPER: String, INPER1: String, INPER2: String, INPOM: String, INPSM: String, IPONDL: String, IRANM: String, METRODOM: String, NBPI: String, RECHM: String, REGION: String, SANI: String, SANIDOM: String, SEXEM: String, STAT_CONJM: String, STOCD: String, SURF: String, TACTM: String, TPM: String, TRANSM: String, TRIRIS: String, TYPC: String, TYPL: String, VOIT: String, WC : String)
  case class Enregistrement_Simple(COMMUNE : String, AEMM : Int, VOIT : Int)

  def main(args: Array[String]): Unit = {

    SparkUtils.sparkConf.registerKryoClasses(Array(Enregistrement_Simple.getClass))

    val sparkSession = SparkUtils.spark()
    val sc = sparkSession.sparkContext

    val rdd = sc.textFile(Constants.LOGEMENT_TXT)
      .filter(l => !l.startsWith("COMMUNE"))
      .map(l => l.split(";", -1))
      .map(l => Enregistrement(l(0), l(1), l(2), l(3), l(4), l(5), l(6), l(7), l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15), l(16), l(17), l(18), l(19), l(20), l(21), l(22), l(23), l(24), l(25), l(26), l(27), l(28), l(29), l(30), l(31), l(32), l(33), l(34), l(35), l(36), l(37), l(38), l(39), l(40), l(41), l(42), l(43), l(44), l(45), l(46), l(47), l(48), l(49), l(50), l(51), l(52), l(53), l(54), l(55), l(56), l(57), l(58), l(59), l(60), l(61), l(62), l(63), l(64), l(65), l(66), l(67), l(68)))
      .cache()

    // On calcule pour chaque commune le nombre total de voitures
    val rddVoituresParLogements =
      rdd
        .map(e => (e.COMMUNE, decodeVOIT(e.VOIT)))
        .groupByKey()
        .mapValues(_.sum)

    // On calcule pour chaque commune l'année moyenne d'emménagement dans le logement
    val rddAnneeEmmenagement =
      rdd
        .map(e => (e.COMMUNE, decodeAEMM(e.AEMM)))
        .groupByKey()
        .mapValues(liste => if (liste.isEmpty) 0 else liste.sum / liste.size)

    // on produit le fichier csv final par jointure
    rddVoituresParLogements
      .join(rddAnneeEmmenagement)
      .map { case (commune, (nb_voitures, aemm_moyen)) => s"$commune;$nb_voitures;$aemm_moyen" }
      .saveAsTextFile(Constants.TMP)


  }

  /**
    * La colonne AEMN contient l'année d'emménagement dans le logement.
    *
    * La valeur est encodée de la manière suivantes :
    * '0000' -> Information non disponible
    * 'ZZZZ' -> Logement dans les DOM-TOM
    * de '1800' à '2014' -> Année emménagement
    */
  def decodeAEMM(aemm : String) = aemm match {
    case "ZZZZ" => 0
    case "0000" => 0
    case _ => aemm.toInt
  }

  /**
    *  La colonne VOIT contient les valeurs suivantes
    *  '0' -> 0 Voiture
    *  '1' -> 1 Voiture
    *  '2' -> 2 Voitures
    *  '3' -> 3 Voitures
    *  'X' -> Inconnu
    *  'Z' -> DOM (logement dans les DOM-TOM)
    */
  def decodeVOIT(voit : String) = voit match {
    case "X" => 0
    case "Z" => 0
    case _ => voit.toInt
  }


  SparkUtils.initExercice("exercice1")

}
