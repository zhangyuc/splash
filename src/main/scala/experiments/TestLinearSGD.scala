package experiments
import splash.optimization._
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Vectors,Vector,DenseVector,SparseVector}
import org.apache.spark.mllib.optimization.{GradientDescent, LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.mllib.util.MLUtils
import java.util.Date

class TestLinearSGD {
  def train(filename:String){
    ////////////////////////// start preparing data ////////////////////////////
    val regParam = 0
    val iters = 1000
    val partition = 64
    var numClasses = 2
    
    var stepsize = 0.0
    var duplication = 1.0
    if(filename.endsWith("covtype.txt")){
      duplication = 16
      stepsize = 1
    }
    if(filename.endsWith("rcv1.txt")){
      duplication = 8
      stepsize = 0.3
    }
    if(filename.endsWith("mnist38.txt")){
      duplication = 1
      stepsize = 0.3
    }
    if(filename.endsWith("kddb.txt")){
      duplication = 1
      stepsize = 0.3
    }
    if(filename.endsWith("mnist8m.txt")){
      duplication = 0.25
      numClasses = 10
      stepsize = 0.3
    }
    if(partition == 1){
      duplication = 1
    }
    
    val conf = new SparkConf().setAppName("Test Linear SGD Application").set("spark.driver.maxResultSize", "12G")
    val sc = new SparkContext(conf)
    val raw_data = sc.textFile(filename).map(line => {
      val tokens = line.split(" ")
      var y = tokens(0).toInt
      if(!filename.endsWith("mnist8m.txt")){
        if(y == 3 || y == -1){
          y = 0
        }
        else if(y == 8){
          y = 1
        }
      }
      
      val x_key = new Array[Int](tokens.length-1)
      val x_value = new Array[Double](tokens.length-1)
      var norm = 0.0
      for(i <- 1 until tokens.length){
        val token_kv = tokens(i).split(":")
        x_key(i-1) = (token_kv(0).toInt-1)
        x_value(i-1) = token_kv(1).toDouble
        norm += x_value(i-1) * x_value(i-1)
      }
      norm = math.sqrt(norm)
      for(i <- 0 until x_value.length){
        x_value(i) = x_value(i) / norm
      }
      (y, x_key, x_value)
    }).cache()
    val dimension = raw_data.map( x => x._2(x._2.length-1) ).reduce( (a,b) => math.max(a, b)) + 1
    val data = raw_data.map( x => (x._1.toDouble, Vectors.sparse(dimension, x._2, x._3)) ).repartition(partition).cache()
    val n = data.count()
    
    println("number of samples = " + n)
    println("dimension = " + dimension)
    ////////////////////////// finished preparing data ////////////////////////////
    val mllib_data = data
    mllib_data.count()
    var startTime = 0.0
    
    /*println("MLLib LBFGS: ")
    startTime = (new Date).getTime
    val correction_num = 10
    val loss_lbfgs = LBFGS.runLBFGS(
      mllib_data,
      new LogisticGradient(numClasses),
      new SquaredL2Updater(),
      correction_num,
      1e-10,
      300,
      regParam,
      Vectors.dense(new Array[Double](dimension*(numClasses-1))))._2
    val mllib_time = ((new Date).getTime - startTime) / 1000
    for(i <- 0 until loss_lbfgs.length){
      println("%5.3f\t%5.8f".format(mllib_time * (i+1) / loss_lbfgs.length, loss_lbfgs(i)))
    }*/
    
    
    println("MLLib SGD: ")
    val miniBatchFraction = 0.01
    val sgd_iter = 10000
    val sgd_stepsize = 1000
    startTime = (new Date).getTime
    val loss_sgd = GradientDescent.runMiniBatchSGD(
      mllib_data,
      new LogisticGradient(numClasses),
      new SquaredL2Updater(),
      sgd_stepsize,
      sgd_iter,
      regParam,
      miniBatchFraction,
      Vectors.dense(new Array[Double](dimension*(numClasses-1))))._2
      
    val mllib_time = ((new Date).getTime - startTime) / 1000
    for(i <- 0 until loss_sgd.length){
      println("%5.3f\t%5.8f".format(mllib_time * (i+1) / loss_sgd.length, loss_sgd(i)))
    }
    // test splash /////////////////////////////////////////////////
    
    println("Splash: ")
    val gradient = {
      if(numClasses == 2) new splash.optimization.LogisticGradient()
      else new splash.optimization.MultiClassLogisticGradient(numClasses)
    }
    val sgd = (new StochasticGradientDescent())
      .setGradient(gradient)
      .setNumIterations(iters)
      .setStepSize(stepsize)
      .setDataPerIteration(duplication)
      .setMaxThreadNum(partition)
      
    val solution = sgd.optimize(data, Vectors.zeros(dimension*(numClasses-1)))
  }
}