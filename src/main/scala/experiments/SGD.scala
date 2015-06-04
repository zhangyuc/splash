package experiments
import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import splash.core._

class SGD {
  def train(filename:String){
    val spc = new SplashConf
    spc.maxThreadNum = 64
    
    var candidate_stepsize = 0.0
    var duplication = 1
    if(filename.endsWith("covtype.txt")){
      duplication = 16
      candidate_stepsize = 20.0
    }
    if(filename.endsWith("rcv1.txt")){
      duplication = 8
      candidate_stepsize = 100.0
    }
    if(filename.endsWith("mnist38.txt")){
      duplication = 1
      candidate_stepsize = 100.0
    }
    
    val stepsize = candidate_stepsize
    val num_of_partition = 64
    val num_of_pass = 1000
    val lambda = 1e-4
    
    if(num_of_partition == 1){
      duplication = 1
    }
    
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
      
      val x_key = new Array[Int](tokens.length-1)
      val x_value = new Array[Double](tokens.length-1)
      var norm = 0.0
      for(i <- 1 until tokens.length){
        val token_kv = tokens(i).split(":")
        x_key(i-1) = token_kv(0).toInt
        x_value(i-1) = token_kv(1).toDouble
        norm += x_value(i-1) * x_value(i-1)
      }
      norm = math.sqrt(norm)
      for(i <- 0 until x_value.length){
        x_value(i) = x_value(i) / norm
      }
      (y,x_key,x_value)
    }).repartition(num_of_partition)
    val dim = data.map( x => x._2(x._2.length-1) ).reduce( (a,b) => math.max(a, b))
        
    // create parametrized RDD
    val paraRdd = new ParametrizedRDD(data).duplicateAndReshuffle(duplication)
    val n = paraRdd.count()
    
    println("Stochastic Gradient Descent")
    println("Data size = " + n + "; dimension = " + dim)
    
    // manager start processing data
    val preprocess = (sharedVar: SharedVariableSet ) => {
      sharedVar.set("nol",n)
      sharedVar.set("lambda",lambda)
      sharedVar.set("dimension",dim)
      sharedVar.set("stepsize", stepsize)
      sharedVar.declareArray("w", dim + 1)
      sharedVar.declareArray("ws", dim + 1)
    }
    
    paraRdd.foreachSharedVariable(preprocess)
    paraRdd.syncSharedVariable()
    paraRdd.process_func = this.update
    paraRdd.evaluate_func = this.evaluateTrainLoss
    
    for( i <- 0 until num_of_pass ){
      paraRdd.foreachSharedVariable(preIterationProcess)
      paraRdd.run(spc)
      val loss = paraRdd.map(evaluateTestLoss).reduce( (a,b) => a+b ) / n
      println("%5.3f\t%5.8f\t".format(paraRdd.totalTimeEllapsed, loss) + paraRdd.lastIterationThreadNumber)
    }
  }
  
  val evaluateTrainLoss = (entry: (Int, Array[Int], Array[Double]), sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    val y = entry._1
    val x_key = entry._2
    val x_value = entry._3
    
    var y_predict = 0.0
    for(i <- 0 until x_key.length){
      y_predict += sharedVar.getArrayElement("ws", x_key(i)) * x_value(i)
    }
    val loss = math.log( 1.0 + math.exp( - y * y_predict ) )
    if( loss < 100 ){
      loss
    }
    else{
      - y * y_predict
    }
  }
  
  val evaluateTestLoss = (entry: (Int, Array[Int], Array[Double]), sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    val y = entry._1
    val x_key = entry._2
    val x_value = entry._3
    
    var y_predict = 0.0
    for(i <- 0 until x_key.length){
      y_predict += sharedVar.getArrayElement("ws", x_key(i)) * x_value(i)
    }
    val loss = math.log( 1.0 + math.exp( - y * y_predict ) )
    if( loss < 100 ){
      loss
    }
    else{
      - y * y_predict
    }
  }
  
  val preIterationProcess = (sharedVar : SharedVariableSet) => {
    sharedVar.setArray("ws", sharedVar.getArray("w"))
    sharedVar.set("count", 0)
  }
  
  val update = (entry: (Int, Array[Int], Array[Double]), weight: Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    val c = sharedVar.get("count")
    val stepsize = sharedVar.get("stepsize")
    val total_c = sharedVar.batchSize
    val y = entry._1
    val x_key = entry._2
    val x_value = entry._3
    val current_weight = weight
    
    var y_predict = 0.0
    for(i <- 0 until x_key.length){
      y_predict += sharedVar.getArrayElement("w", x_key(i)) * x_value(i)
    }

    sharedVar.add("t", current_weight)
    val t = sharedVar.get("t")
    for(i <- 0 until x_key.length)
    {
      val delta = stepsize * current_weight / math.sqrt(t) * y / (1.0 + math.exp(y*y_predict)) * x_value(i)
      sharedVar.addArrayElement("w", x_key(i), delta)
      sharedVar.addArrayElement("ws", x_key(i), delta * (total_c - c) / total_c)
    }
    sharedVar.add("count", current_weight)
  }
}