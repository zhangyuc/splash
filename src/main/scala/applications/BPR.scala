import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import java.io.File
import scala.io.Source
import splash.StreamProcessContext
import splash._

class BPR {
  def train(filename:String) {
    val spc = new StreamProcessContext
    spc.numOfThread = 64
    spc.applyAdaptiveReweighting = true
    spc.reweight = 1
    spc.adaptiveReweightingSampleRatio = 0.1
    
    val num_of_partition = 64
    val num_of_pass = 1000
    val dimension = 10
    val lambda = 0.01
    
    val conf = new SparkConf().setAppName("BPR Application")
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

    
    val preprocess = (sharedVar:ParameterSet ) => {
      sharedVar.set("num_of_item",num_of_item)
      sharedVar.set("dimension", dimension)
      sharedVar.set("lambda", lambda)
      sharedVar.set("num_of_partition", num_of_partition)
    }
    
    // take several passes over the dataset
    val paraRdd = new ParametrizedRDD[(Int, (Int, Boolean))]( data )
    paraRdd.process_func = this.update
    // paraRdd.evaluate_func = this.evaluateTrainLoss
    
    paraRdd.foreachSharedVariable(preprocess)
    paraRdd.foreachSharedVariable(initialize)
    paraRdd.syncSharedVariable()
    println("initialization complete")
    
    for( i <- 0 until num_of_pass ){
      paraRdd.streamProcess(spc)
      val loss = paraRdd.map(evaluateTestLoss).reduce( (a,b) => a+b ) / testFreq
      println("%5.3f\t%5.8f\t%d".format(paraRdd.totalTimeEllapsed, 1-loss, paraRdd.proposedWeight.toInt))
    }
  }
  
  val initialize = (sharedVar : ParameterSet) => {
    val dim = sharedVar.get("dimension").toInt
    val nop = sharedVar.get("num_of_partition")
    val num_of_item = sharedVar.get("num_of_item").toInt
    val rnd = new Random
    
    for(item_id <- 1 until num_of_item + 1)
    {
      for(i <- 0 until dim){
        sharedVar.update("I:" + item_id + ":" + i, rnd.nextGaussian() * 0.1, UpdateType.Push)
      }
    }
  }
  
  val evaluateTrainLoss = (entry: (Int, (Int, Boolean)), sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    if(entry._2._2 == true){
      val dim = sharedVar.get("dimension").toInt
      val num_of_item = sharedVar.get("num_of_item").toInt
      val user_id = entry._1
      val item_id = entry._2._1
      val alt_item_id = Random.nextInt(num_of_item) + 1
      val vu = new Array[Double](dim)
      val vi = new Array[Double](dim)
      val vj = new Array[Double](dim)
      
      for(i <- 0 until dim){
        vu(i) = sharedVar.get("U:" + user_id + ":" + i)
        vi(i) = sharedVar.get("I:" + item_id + ":" + i)
        vj(i) = sharedVar.get("I:" + alt_item_id + ":" + i)
      }
      
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
  
  val evaluateTestLoss = (entry: (Int, (Int, Boolean)), sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    if(entry._2._2 == false){
      val dim = sharedVar.get("dimension").toInt
      val num_of_item = sharedVar.get("num_of_item").toInt
      val user_id = entry._1
      val item_id = entry._2._1
      val alt_item_id = Random.nextInt(num_of_item) + 1
      val vu = new Array[Double](dim)
      val vi = new Array[Double](dim)
      val vj = new Array[Double](dim)
      
      for(i <- 0 until dim){
        vu(i) = sharedVar.get("U:" + user_id + ":" + i)
        vi(i) = sharedVar.get("I:" + item_id + ":" + i)
        vj(i) = sharedVar.get("I:" + alt_item_id + ":" + i)
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
  
  val update = ( rnd: Random, entry: (Int, (Int, Boolean)), weight: Double, sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    if(entry._2._2 == true){
      var total_weight = weight.toInt
      var current_weight = 1
      val lambda = sharedVar.get("lambda")
      val dim = sharedVar.get("dimension").toInt
      val num_of_item = sharedVar.get("num_of_item").toInt
      val user_id = entry._1
      val item_id = entry._2._1
      val alt_item_id = rnd.nextInt(num_of_item) + 1
      val vu = new Array[Double](dim)
      val vi = new Array[Double](dim)
      val vj = new Array[Double](dim)
      val vu_old = new Array[Double](dim)
      val vi_old = new Array[Double](dim)
      val vj_old = new Array[Double](dim)
      for(i <- 0 until dim){
        vu(i) = sharedVar.get("U:" + user_id + ":" + i)
        vi(i) = sharedVar.get("I:" + item_id + ":" + i)
        vj(i) = sharedVar.get("I:" + alt_item_id + ":" + i)
        vu_old(i) = vu(i)
        vi_old(i) = vi(i)
        vj_old(i) = vj(i)
      }
      
      while(total_weight > 0){
        val alpha = 0.01 * current_weight
        
        // compute inner product
        var inner_product = 0.0
        for(i <- 0 until dim){
          inner_product += vu(i) * (vi(i) - vj(i))
        }
        val scalar = 1.0 / (1.0 + math.exp(inner_product))
        
        // stochastic gradient descent
        for(i <- 0 until dim){
          vu(i) += alpha * (scalar * (vi(i) - vj(i)) - lambda * vu(i))
          vi(i) += alpha * (scalar * vu(i) - lambda * vi(i))
          vj(i) += alpha * ( - scalar * vu(i) - lambda * vj(i))
        }
        
        // adjust weight
        total_weight -= current_weight
        if(total_weight >= current_weight * 4){
          current_weight = current_weight * 4
        }
        else{
          current_weight = total_weight
        }
      }
      
      for(i <- 0 until dim){
        sharedVar.update("U:" + user_id + ":" + i, vu(i) - vu_old(i), UpdateType.Keep )
        sharedVar.update("I:" + item_id + ":" + i, vi(i) - vi_old(i), UpdateType.Push )
        sharedVar.update("I:" + alt_item_id + ":" + i, vj(i) - vj_old(i), UpdateType.Push )
      }
    }
  }
}

// data generator
object BPRDataGenerator{
  def main(args: Array[String]) {
    val files = new File("/home/yuczhang/Downloads/download/training_set").listFiles
    for(file <- files){
      var count = 0
      var movie_id = 0
      for(line <- Source.fromFile(file).getLines()){
        if(count == 0){
          movie_id = line.split(":")(0).toInt
        }
        else{
          val user_id = line.split(",")(0).toInt
          println(user_id + " " + movie_id)
        }
        count += 1
      }
    }
  }
}