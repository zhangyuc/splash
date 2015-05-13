import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import java.util.Date

class GD {
  def train(filename:String) {
    val num_of_partition = 64
    val num_of_pass = 1000
    
    // 2.0 for covtype, 20.0 for mnist38, 20.0 for rcv1
    var candidate_stepsize = 0.0
    if(filename.endsWith("covtype.txt")){
      candidate_stepsize = 20.0
    }
    if(filename.endsWith("rcv1.txt")){
      candidate_stepsize = 20.0
    }
    if(filename.endsWith("mnist38.txt")){
      candidate_stepsize = 20.0
    }
    val stepsize = candidate_stepsize
    
    // Load and parse the data file
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
    val n = data.count()
    val dim = data.map( x => x._2(x._2.length-1) ).reduce( (a,b) => math.max(a, b))

    
    println("Mini-batch Gradient Descent")
    // manager start processing data
    var weight = new Array[Double](dim+1)
    
    var totalTime = 0.0
    for( i <- 0 until num_of_pass ){
      val w_before = weight
      val startTime = (new Date()).getTime
      val dimension = dim
      // calculate global gradient
      val gradient = data.mapPartitions( iter => {
        val gradient = new Array[Double](dimension+1)
        while(iter.hasNext){
          val line = iter.next()
          val y = line._1
          val x_key = line._2
          val x_value = line._3
            
          var y_predict = 0.0
          for(i <- 0 until x_key.length){
            y_predict += w_before(x_key(i)) * x_value(i)
          }
          for(i <- 0 until x_key.length){
            gradient(x_key(i)) += y / (1.0 + math.exp(y*y_predict)) * x_value(i)
          }
        }
        Array(gradient).iterator
      }).reduce( (g1,g2) => {
        for(i <- 0 until g1.length){
          g1(i) += g2(i)
        }
        g1
      })
      
      // gradient descent
      for(i <- 0 until gradient.length){
        weight(i) += stepsize * gradient(i) / n
      }
      val endTime = (new Date()).getTime
      totalTime += (endTime - startTime).toDouble / 1000
      
      // evaluate loss
      val w_after = weight
      val loss = data.map( line => {
        val y = line._1
        val x_key = line._2
        val x_value = line._3
          
        var y_predict = 0.0
        for(i <- 0 until x_key.length){
          y_predict += w_after(x_key(i)) * x_value(i)
        }
        math.log( 1.0 + math.exp( - y * y_predict) )
      }).reduce((a,b) => a+b) / n
      
      // print loss
      println("%5.3f\t%5.5f".format(totalTime, loss))
    }
  }
}