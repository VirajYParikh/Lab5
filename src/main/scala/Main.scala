import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.XML

object XMLReader {

  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val conf = new SparkConf().setAppName("XML Reader")
    // Create Spark context
    val sc = new SparkContext(conf)

    // Load XML files
    val fileRdd = sc.wholeTextFiles("hdfs:///user/vp2359_nyu_edu/loudacre/activations/activations/*.xml")

    // Parse XML files and extract activation records
    // val seqRdd = fileRdd.flatMap { case (_, xmlString) =>
    //   val xml = XML.loadString(xmlString)
    //   (xml \ "activation").map { activation =>
    //     ((activation \ "account-number").text + ":" + (activation \ "model").text)
    //   }
    // }

    val seqRdd = fileRdd.map(x => XML.loadString(x._2) \ "activation")
    val actRdd = seqRdd.flatMap(identity)
    val tupleRdd = actRdd.map(x => ((x \ "model").text + ":" +  (x \ "account-number").text))

    val outputFilePath = "hdfs:///user/vp2359_nyu_edu/loudacre/accounts-models/"
    // Save to HDFS
    tupleRdd.saveAsTextFile(outputFilePath)

    // Print sample records
    tupleRdd.take(20).foreach(println)

    // Stop Spark context
    sc.stop()
  }
}
