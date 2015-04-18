import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import splash._

class LDA {
  
  val train = (vocfile:String, docfile:String) => {
    val spc = new StreamProcessContext
    spc.threadNum = 64
    spc.adaptiveWeightFoldNum = 1
    spc.weight = 1
    spc.warmStart = false
    
    val num_of_pass = 100
    val num_of_partition = 64
    val num_of_topic = 100
    val alpha = 50.0 / num_of_topic
    val beta = 0.01
    val conf = new SparkConf().setAppName("LDA-Gibbs Application").set("spark.driver.maxResultSize", "3G").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    
    // read data and repartition
    val data = sc.textFile(docfile).flatMap( a => {
      val tokens = a.split(" ")
      val list = new ListBuffer[(Int,(Int,Boolean))]
      if(tokens.length == 3){
        for(i <- 0 until tokens(2).toInt){
          if(Random.nextDouble() < 0.5){
            list.append((tokens(0).toInt, (tokens(1).toInt, true)))
          }
          else{
            list.append((tokens(0).toInt, (tokens(1).toInt, false)))
          }
        }
      }
      list.iterator
    }).repartition(num_of_partition).cache()
    
    val freq = data.count()
    val testFreq = data.filter( a => (a._2._2 == false) ).count()
    val trainFreq = freq - testFreq
    
    // read vocabulary
    val vocabulary = new HashMap[Int, String]
    val voc_array = sc.textFile(vocfile).collect()
    var index = 1
    for(line <- voc_array){
      vocabulary.put(index, line)
      index += 1
    }
    val voc_size = vocabulary.count( a => true)
    
    // print basic information
    println("Latent Dirichlet Allocation")
    println("vocabulary size = " + voc_size)
    println("found " + trainFreq + " tokens for training and " + testFreq + " tokens for testing.")
    
    // manager start processing data
    val preprocess = (sharedVar:ParameterSet) => {
      sharedVar.set("voc_size", voc_size)
      sharedVar.set("num_of_topic", num_of_topic)
      sharedVar.set("alpha",alpha)
      sharedVar.set("beta",beta)
    }
    val paraRdd = new ParametrizedRDD(data, true)
    paraRdd.process_func = this.update
    paraRdd.evaluate_func = this.evaluateTrainLoss
    
    paraRdd.foreachSharedVariable(preprocess)
    paraRdd.foreach(initialize)
    paraRdd.syncSharedVariable()

    // take several passes over the dataset
    for(i <- 0 until num_of_pass){  
      paraRdd.streamProcess(spc)
      val loss = math.exp( paraRdd.map(evaluateTestLoss).reduce( (a,b) => a+b ) / testFreq )
      println("%5.3f\t%5.5f\t%f".format(paraRdd.totalTimeEllapsed, loss, paraRdd.proposedWeight))
    }
    
    // view topics and their top words
    val sharedVar = paraRdd.getFirstSharedVariable()
    val wordCount = new Array[ListBuffer[(Int, Double)]](num_of_topic)
    for(tid <- 0 until num_of_topic){
      wordCount(tid) = new ListBuffer[(Int, Double)]
    }
    for(kv_pair <- sharedVar.variable){
      if(kv_pair._1.startsWith("w:")){
        val tokens = kv_pair._1.split(":")
        wordCount(tokens(1).toInt).append((tokens(2).toInt, kv_pair._2)) 
      }
    }
    for(tid <- 0 until num_of_topic){
      wordCount(tid) =  wordCount(tid).sortWith( (a,b) => a._2 > b._2 )
      print("topic " + tid + ": ")
      for(i <- 0 until math.min(20, wordCount(tid).length)){
        print( vocabulary(wordCount(tid)(i)._1) + "(" + wordCount(tid)(i)._2 + ") " )
      }
      println()
      println()
    }
  }
  
  val evaluateTrainLoss = (entry:(Int,(Int,Boolean)), sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    if(entry._2._2 == true)
    {
      val voc_size = sharedVar.get("voc_size")
      val num_of_topic = sharedVar.get("num_of_topic").toInt
      val alpha = sharedVar.get("alpha")
      val beta = sharedVar.get("beta")
      val doc_id = entry._1
      val word_id = entry._2._1
      
      // calculate the probability that this word belonging to each topic
      val td_all = sharedVar.get("da:" + doc_id)
      var sum_prob = 0.0
      for(tid <- 0 until num_of_topic){
        val tw = sharedVar.get("w:"+tid+":"+word_id)
        val tw_all = sharedVar.get("wa:" + tid)
        val td = sharedVar.get("d:"+tid+":"+doc_id)
        sum_prob += (td + alpha) / (td_all + alpha * num_of_topic) * (tw + beta) / (tw_all + beta * voc_size)
      }
      - math.log(sum_prob)
    }
    else{
      0.0
    }
  }
  
  val evaluateTestLoss = (entry:(Int,(Int,Boolean)), sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    if(entry._2._2 == false)
    {
      val voc_size = sharedVar.get("voc_size")
      val num_of_topic = sharedVar.get("num_of_topic").toInt
      val alpha = sharedVar.get("alpha")
      val beta = sharedVar.get("beta")
      val doc_id = entry._1
      val word_id = entry._2._1
      
      // calculate the probability that this word belonging to each topic
      val td_all = sharedVar.get("da:" + doc_id)
      var sum_prob = 0.0
      for(tid <- 0 until num_of_topic){
        val tw = sharedVar.get("w:"+tid+":"+word_id)
        val tw_all = sharedVar.get("wa:" + tid)
        val td = sharedVar.get("d:"+tid+":"+doc_id)
        sum_prob += (td + alpha) / (td_all + alpha * num_of_topic) * (tw + beta) / (tw_all + beta * voc_size)
      }
      - math.log(sum_prob)
    }
    else{
      0.0
    }
  }
  
  val initialize = (entry:(Int,(Int,Boolean)), sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    if(entry._2._2 == true)
    {
      val num_of_topic = sharedVar.get("num_of_topic").toInt
      val doc_id = entry._1
      val word_id = entry._2._1
      val word_freq = 1.0
      
      val init_topic = Random.nextInt(num_of_topic)
      sharedVar.update( "w:"+init_topic+":"+word_id, word_freq)
      sharedVar.update( "wa:" + init_topic, word_freq)
      sharedVar.update("d:"+init_topic+":"+doc_id, word_freq)
      sharedVar.update("da:" + doc_id, word_freq )
      localVar.set("T", init_topic + 1 )
    }
  }
  
  
  val update = ( rnd: Random, entry:(Int,(Int,Boolean)), weight : Double, sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    if(entry._2._2 == true)
    {
      val voc_size = sharedVar.get("voc_size")
      val num_of_topic = sharedVar.get("num_of_topic").toInt
      val alpha = sharedVar.get("alpha")
      val beta = sharedVar.get("beta")
      val doc_id = entry._1
      val word_id = entry._2._1
      
      val old_topic = localVar.get("T").toInt - 1
      if(old_topic >= 0){
        sharedVar.updateWithUnitWeight( "w:"+old_topic+":"+word_id, - 1)
        sharedVar.updateWithUnitWeight( "wa:" + old_topic, - 1)
        sharedVar.updateWithUnitWeight( "d:"+old_topic+":"+doc_id, - 1)
        sharedVar.updateWithUnitWeight( "da:" + doc_id, - 1)
      }
      
      // calculate the probability that this word belongs to some topic
      val prob = new Array[Double](num_of_topic)
      var sum_prob = 0.0
      for(tid <- 0 until num_of_topic){
        val tw = sharedVar.get("w:"+tid+":"+word_id)
        val tw_all = sharedVar.get("wa:" + tid)
        val td = sharedVar.get("d:"+tid+":"+doc_id)
        prob(tid) = (td + alpha) * (tw + beta) / (tw_all + beta * voc_size)
        sum_prob += prob(tid)
      }
      for(tid <- 0 until num_of_topic){
        prob(tid) /= sum_prob
      }
      
      // sample the new topic for this word
      val rand = rnd.nextDouble()
      var new_topic = 0
      var accu_prob = prob(0)
      while( rand >= accu_prob){
        new_topic += 1
        accu_prob += prob(new_topic)
      }
      
      // update shared variables
      sharedVar.update( "w:"+new_topic+":"+word_id, weight)
      sharedVar.update( "wa:" + new_topic, weight)
      sharedVar.update( "d:"+new_topic+":"+doc_id, weight)
      sharedVar.update( "da:" + doc_id, weight)
      
      // update local variable
      localVar.set("T", new_topic + 1 )
    }
  }
}