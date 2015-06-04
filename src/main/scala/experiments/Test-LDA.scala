package experiments
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable._
import scala.util.Random
import splash.sampling._

class TestLDA {
  val train = (docfile:String) => {
    val conf = new SparkConf().setAppName("LDA-Gibbs Application").set("spark.driver.maxResultSize", "6G")
    val sc = new SparkContext(conf)
    val numPartitions = 64
    
    // LDA parameters
    val numTopics = 100
    val alpha = 50.0 / numTopics
    val beta = 0.01
    
    // initialize every word in the corpus by a random topic
    val corpusWithRandomTopics = sc.textFile(docfile).repartition(numPartitions).flatMap( a => {
      val tokens = a.split(" ")
      if(tokens.length == 3){
        val docId = tokens(0).toInt-1
        val wordId = tokens(1).toInt-1
        val wordCount = tokens(2).toInt
        val topicId = Random.nextInt(numTopics)
        val list = new ListBuffer[(Int, WordToken)]
        for(i <- 0 until wordCount) list.append( (docId, new WordToken(wordId, 1, Random.nextInt(numTopics))) )
        list.iterator
      } 
      else Array().iterator
    }).cache()
    
    // train LDA
    val corpusWithSampledTopics = (new CollapsedGibbsSamplingForLDA)
      .setNumTopics(numTopics)
      .setAlphaBeta((alpha,beta))
      .setNumIterations(1000)
      .sample(corpusWithRandomTopics)
  }
}