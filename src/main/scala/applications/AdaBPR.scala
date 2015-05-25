import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import java.io.File
import scala.io.Source
import splash.StreamProcessContext
import splash._

class AdaBPR {
  def train(filename:String) {
    val spc = new StreamProcessContext
    spc.threadNum = 64
    spc.warmStart = false
    
    val num_of_partition = 64
    val num_of_pass = 1000
    val dimension = 10
    val lambda = 0.01
    
    val conf = new SparkConf().setAppName("BPR Application").set("spark.driver.maxResultSize", "6G")
    val sc = new SparkContext(conf)
    var data = sc.textFile(filename,num_of_partition).map( line => {
      val tokens = line.split(" ")
      if(Random.nextDouble() < 0.5){
        (tokens(0).toInt, (tokens(1).toInt, true))
      }
      else{
        (tokens(0).toInt, (tokens(1).toInt, false))
      }
    })
    
    data = data.partitionBy(new HashPartitioner(num_of_partition)).cache()
    val n = data.count()
    val num_of_item = data.map( x => x._2._1 ).reduce( (x,y) => math.max(x, y))
    val testFreq = data.filter( a => (a._2._2 == false) ).count()

    println("Bayesian Personalized Ranking")
    println("found " + num_of_item + " items.")
    println("found " + (n - testFreq) + " tokens for training and " + testFreq + " tokens for testing.")
    
    val preprocess = (sharedVar: SharedVariableSet) => {
      sharedVar.set("num_of_item",num_of_item)
      sharedVar.set("dimension", dimension)
      sharedVar.set("lambda", lambda)
      sharedVar.set("num_of_partition", num_of_partition)
      
      val rnd = new Random
      for(item_id <- 1 until num_of_item + 1)
      {
        val vi = new Array[Double](dimension + 1)
        for(i <- 0 until dimension){
          vi(i) = rnd.nextGaussian() * 0.1
        }
        sharedVar.declareArray("I:" + item_id, dimension + 1)
        sharedVar.addArray("I:" + item_id, vi)
      }
    }
    
    // take several passes over the dataset
    val paraRdd = new ParametrizedRDD(data, true)
    paraRdd.process_func = this.update
    paraRdd.evaluate_func = this.evaluateTrainLoss
    
    paraRdd.foreachSharedVariable(preprocess)
    paraRdd.syncSharedVariable()
    println("initialization complete")
    
    for( i <- 0 until num_of_pass ){
      paraRdd.run(spc)
      val loss = paraRdd.map(evaluateTestLoss).reduce( (a,b) => a+b ) / testFreq 
      println("%5.3f\t%5.8f\t%d".format(paraRdd.totalTimeEllapsed, 1-loss, paraRdd.proposedGroupNum.toInt))
    }
  }
  
  val evaluateTrainLoss = (entry: (Int, (Int, Boolean)), sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    if(entry._2._2 == true){
      val dim = sharedVar.get("dimension").toInt
      val num_of_item = sharedVar.get("num_of_item").toInt
      val user_id = entry._1
      val item_id = entry._2._1
      val alt_item_id = Random.nextInt(num_of_item) + 1
      val vu = sharedVar.getArray("U:" + user_id)
      val vi = sharedVar.getArray("I:" + item_id)
      val vj = sharedVar.getArray("I:" + alt_item_id)
      
      // compute inner product
      var inner_product = 0.0
      for(i <- 0 until dim){
        inner_product += vu(i) * (vi(i) - vj(i))
      }
      val loss = math.log( 1.0 + math.exp(- inner_product ) )
      if(loss.isInfinite()){
        -inner_product
      }
      else{
        loss
      }
    }
    else{
      0.0
    }
  }
  
  val evaluateTestLoss = (entry: (Int, (Int, Boolean)), sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    if(entry._2._2 == false){
      val dim = sharedVar.get("dimension").toInt
      val num_of_item = sharedVar.get("num_of_item").toInt
      val user_id = entry._1
      val item_id = entry._2._1
      val alt_item_id = Random.nextInt(num_of_item) + 1
      var vu = sharedVar.getArray("U:" + user_id)
      val vi = sharedVar.getArray("I:" + item_id)
      val vj = sharedVar.getArray("I:" + alt_item_id)
      if(vu == null){
        sharedVar.declareArray("U:" + user_id, dim + 1)
        vu = new Array[Double](dim)
      }
      
      // compute inner product
      var inner_product = 0.0
      for(i <- 0 until dim){
        inner_product += vu(i) * (vi(i) - vj(i))
      }
      
      if(inner_product > 0){
        0.0
      }
      else if(inner_product < 0){
        1.0
      }
      else{
        0.5
      }
    }
    else{
      0.0
    }
  }
  
  val update = (entry: (Int, (Int, Boolean)), weight: Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    if(entry._2._2 == true){
      val current_weight = weight
      val lambda = sharedVar.get("lambda")
      val dim = sharedVar.get("dimension").toInt
      val num_of_item = sharedVar.get("num_of_item").toInt
      val user_id = entry._1
      val item_id = entry._2._1
      val alt_item_id = Random.nextInt(num_of_item) + 1
      var vu = sharedVar.getArray("U:" + user_id)
      val vi = sharedVar.getArray("I:" + item_id)
      val vj = sharedVar.getArray("I:" + alt_item_id)
      if(vu == null){
        sharedVar.declareArray("U:" + user_id, dim + 1)
        vu = new Array[Double](dim + 1)
      }
      
      val vu_delta = new Array[Double](dim + 1)
      val vi_delta = new Array[Double](dim + 1)
      val vj_delta = new Array[Double](dim + 1)
      
      val alpha = 0.5
      val alpha_u = math.min(0.5, alpha * current_weight / math.sqrt(1e-8 + vu(dim)))
      val alpha_i = math.min(0.5, alpha * current_weight / math.sqrt(1e-8 + vi(dim)))
      val alpha_j = math.min(0.5, alpha * current_weight / math.sqrt(1e-8 + vj(dim)))
      
      // compute inner product
      var inner_product = 0.0
      for(i <- 0 until dim){
        inner_product += vu(i) * (vi(i) - vj(i))
      }
      val scalar = 1.0 / (1.0 + math.exp(inner_product))
      
      // stochastic gradient descent
      for(i <- 0 until dim){
        // gradient
        vu_delta(i) = scalar * (vi(i) - vj(i)) - lambda * vu(i)
        vi_delta(i) = scalar * vu(i) - lambda * vi(i)
        vj_delta(i) = - scalar * vu(i) - lambda * vj(i)
        
        // accumulate gradient
        vu_delta(dim) += math.pow(vu_delta(i), 2)
        vi_delta(dim) += math.pow(vi_delta(i), 2)
        vj_delta(dim) += math.pow(vj_delta(i), 2)
      }
      
      // multiply stepsize
      for(i <- 0 until dim){
        vu_delta(i) *= alpha_u
        vi_delta(i) *= alpha_i
        vj_delta(i) *= alpha_j
      }
      vu_delta(dim) *= current_weight
      vi_delta(dim) *= current_weight
      vj_delta(dim) *= current_weight
      
      sharedVar.addArray("U:" + user_id, vu_delta)
      sharedVar.dontSyncArray("U:" + user_id)
      sharedVar.addArray("I:" + item_id, vi_delta)
      sharedVar.addArray("I:" + alt_item_id, vj_delta)
    }
  }
}