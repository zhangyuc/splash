import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import java.util.Date

class GD {
  def train(filename:String) {
    val num_of_partition = 64
    val num_of_pass = 1000
    val stepsize = 20.0 // 2.0 for covtype, 20.0 for mnist38, 20.0 for rcv1
      
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
      val x = new HashMap[Int, Double]
      var norm = 0.0
      for(i <- 1 until tokens.length){
        val token_kv = tokens(i).split(":")
        x.put(token_kv(0).toInt, token_kv(1).toDouble)
        norm += token_kv(1).toDouble * token_kv(1).toDouble
      }
      norm = math.sqrt(norm)
      for(key <- x.keySet){
        x(key) = x(key) / norm
      }
      (y,x)
    }).repartition(num_of_partition)
    val n = data.count()
    
    println("Mini-batch Gradient Descent")
    // manager start processing data
    var weight = new HashMap[Int, Double]
    var totalTime = 0.0
    for( i <- 0 until num_of_pass ){
      val w_before = sc.broadcast(weight)
      val startTime = (new Date()).getTime
      // calculate global gradient
      val gradient = data.mapPartitions( iter => {
        val g = new HashMap[Int, Double]
        while(iter.hasNext){
          val line = iter.next()
          val y = line._1
          val x = line._2
          
          var y_predict = 0.0
          for(pair <- x){
            y_predict += w_before.value.applyOrElse(pair._1, (a:Any)=>0.0) * pair._2
          }
          for(pair <- x){
            g.put(pair._1, g.applyOrElse(pair._1, (a:Any)=> 0.0) - y / (1.0 + math.exp(y*y_predict)) * pair._2 )
          }
        }
        Array(g).iterator
      }).reduce( (g1,g2) => {
        for(pair <- g2){
          g1.put(pair._1, g1.applyOrElse(pair._1, (a:Any)=>0.0) + pair._2)
        }
        g1
      })
      
      // gradient descent
      for(pair <- gradient){
        weight.put(pair._1, weight.applyOrElse(pair._1, (a:Any)=>0.0) - stepsize * pair._2 / n)
      }
      val endTime = (new Date()).getTime
      totalTime += (endTime - startTime).toDouble / 1000
      
      // evaluate loss
      val w_after = sc.broadcast(weight)
      val loss = data.map( line => {
        val y = line._1
        val x = line._2
        
        var y_predict = 0.0
        for(pair <- x){
          y_predict += w_after.value.applyOrElse(pair._1, (a:Any)=>0.0) * pair._2
        }
        math.log( 1.0 + math.exp( - y * y_predict) )
      }).reduce((a,b) => a+b) / n
      
      // print loss
      println("%5.3f\t%5.5f".format(totalTime, loss))
    }
  }
}