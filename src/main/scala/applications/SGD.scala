import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import splash._

class SGD {
  def train(filename:String){
    val spc = new StreamProcessContext
    spc.numOfThread = 64
    spc.batchSize = 1.0
    spc.applyAdaptiveReweighting = true
    spc.reweight = 64
    spc.adaptiveReweightingSampleRatio = 0.1
    
    val num_of_partition = 64
    val num_of_pass = 1000
    val lambda = 1e-4
    
    val conf = new SparkConf().setAppName("SGD Application")
    val sc = new SparkContext(conf)
    val data = sc.textFile(filename).map( line => {
      val tokens = line.split(" ")
      var y = tokens(0).toInt
      if(y == 3){
        y = -1
      }
      else if(y == 8){
        y = 1
      } 
      
      val x_key = new Array[String](tokens.length-1)
      val x_value = new Array[Double](tokens.length-1)
      var norm = 0.0
      for(i <- 1 until tokens.length){
        val token_kv = tokens(i).split(":")
        x_key(i-1) = token_kv(0)
        x_value(i-1) = token_kv(1).toDouble
        norm += x_value(i-1) * x_value(i-1)
      }
      norm = math.sqrt(norm)
      for(i <- 0 until x_value.length){
        x_value(i) = x_value(i) / norm
      }
      (y,x_key,x_value)
    }).repartition(num_of_partition)
    val n = data.count()
    
    println("Stochastic Gradient Descent")
    
    // manager start processing data
    val preprocess = (sharedVar:ParameterSet ) => {
      sharedVar.set("nol",n)
      sharedVar.set("lambda",lambda)
    }
    
    // take several passes over the dataset
    val paraRdd = new ParametrizedRDD[(Int, Array[String], Array[Double])]( data )
    paraRdd.foreachSharedVariable(preprocess)
    paraRdd.process_func = this.update
    paraRdd.evaluate_func = this.evaluateTestLoss
    
    for( i <- 0 until num_of_pass ){
      paraRdd.foreachSharedVariable(preIterationProcess)
      paraRdd.streamProcess(spc)
      val loss = paraRdd.map(evaluateTestLoss).reduce( (a,b) => a+b ) / n
      println("%5.3f\t%5.8f\t%f".format(paraRdd.totalTimeEllapsed, loss, paraRdd.proposedWeight))
    }
    
    // output
    val sharedVar = paraRdd.getFirstSharedVariable()
    SimpleApp.print_values(sharedVar)
    //stackedRdd.toRDD().saveAsTextFile(filename + ".post")
  }
  
  val evaluateTestLoss = (entry: (Int, Array[String], Array[Double]), sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    val y = entry._1
    val x_key = entry._2
    val x_value = entry._3
    
    var y_predict = 0.0
    for(i <- 0 until x_key.length){
      y_predict += sharedVar.get("ws:"+x_key(i)) * x_value(i)
    }
    val loss = math.log( 1.0 + math.exp( - y * y_predict ) )
    if( loss < 100 ){
      loss
    }
    else{
      - y * y_predict
    }
  }
  
  val preIterationProcess = (sharedVar : ParameterSet) => {
    for(key <- sharedVar.variable.keySet){
      if(key.startsWith("w:")){
        val index = key.split(":")(1)
        sharedVar.set("ws:"+index, sharedVar.get(key))
      }
    }
    sharedVar.set("count", 0)
  }
  
  val update = ( rnd: Random, entry: (Int, Array[String], Array[Double]), 
      weight: Double, sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    val t = sharedVar.get("t")
    val c = sharedVar.get("count")
    val total_c = sharedVar.getBatchSize()
    val y = entry._1
    val x_key = entry._2
    val x_value = entry._3
    
    var y_predict = 0.0
    for(i <- 0 until x_key.length){
      y_predict += sharedVar.get("w:"+x_key(i)) * x_value(i)
    }

    // update primal vector
    // stepsize 100 for rcv1 and mnist38, 20 for covtype
    for(i <- 0 until x_key.length)
    {
      val delta = 20.0 * weight / math.sqrt( t + 1 ) * y / (1.0 + math.exp(y*y_predict)) * x_value(i)
      sharedVar.update("w:"+x_key(i), delta, UpdateType.Push )
      sharedVar.update("ws:"+x_key(i), delta * (total_c - c) / total_c, UpdateType.Push )
    }
    sharedVar.update("t", weight, UpdateType.Push )
    sharedVar.update("count", weight, UpdateType.Keep )
  }
}