package experiments
import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import java.io.File
import java.util.Date

class ALM {
  def train(filename:String) {
    val num_of_partition = 64
    val num_of_pass = 1000
    val dimension = 10
    
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
    }).cache()
    
    val n = data.count()
    val testFreq = data.filter( a => (a._2._2 == false) ).count()
    val user_indexed_data = data.partitionBy(new HashPartitioner(num_of_partition)).cache()
    val item_indexed_data = data.map( x => (x._2._1, (x._1, x._2._2))).partitionBy(new HashPartitioner(num_of_partition)).cache()
    
    val user_ids = (new HashSet[Int]) ++ user_indexed_data.keys.collect()
    val item_ids = (new HashSet[Int]) ++ item_indexed_data.keys.collect()
    
    var user_vec = new HashMap[Int, Array[Double]]
    var item_vec = new HashMap[Int, Array[Double]]
    val num_of_user = user_ids.count { x => true }
    val num_of_item = item_ids.count { x => true }
    
    println("Batch Recommender System")
    println("found " + num_of_user + " users.")
    println("found " + num_of_item + " items.")
    println("found " + (n - testFreq) + " tokens for training and " + testFreq + " tokens for testing.")
    
    // initialization
    val rnd = new Random
    
    // initialize user vector
    for(uid <- user_ids){
      val array = new Array[Double](dimension)
      for(i <- 0 until dimension){
        array(i) = rnd.nextGaussian() * 0.1
      }
      user_vec.put(uid, array)
    }
    
    // initialize item vector
    for(iid <- item_ids){
      val array = new Array[Double](dimension)
      for(i <- 0 until dimension){
        array(i) = rnd.nextGaussian() * 0.1
      }
      item_vec.put(iid, array)
    }
    
    var totalTime = 0.0
    // alternating loss minimization
    for(iter <- 0 until num_of_pass){
      val user_broadcast = user_vec
      val item_broadcast = item_vec
      val dim = dimension
      val lambda = 0.01
      val alpha = 0.1
      val beta = 1e-4
      val nitem = num_of_item
      
      val startTime = (new Date()).getTime
      // recompute user vectors
      val new_user_vec_array = user_indexed_data.mapPartitions( iterator => {
        val new_user_vec = new HashMap[Int, Array[Double]]
        
        while(iterator.hasNext){
          val entry = iterator.next()
          if(entry._2._2 == true){
            val user_id = entry._1
            val item_id = entry._2._1
            val alt_item_id = Random.nextInt(nitem) + 1
            
            val vu = new_user_vec.applyOrElse(user_id, (x:Int) => user_broadcast(x))
            val vi = item_broadcast(item_id)
            val vj = item_broadcast(alt_item_id)
            
            // compute inner product
            var inner_product = 0.0
            for(i <- 0 until dim){
              inner_product += vu(i) * (vi(i) - vj(i))
            }
            val scalar = 1.0 / (1.0 + math.exp(inner_product))
            
            // gradient descent
            for(i <- 0 until dim){
              vu(i) += alpha * (scalar * (vi(i) - vj(i)) - lambda * vu(i))
            }
            new_user_vec.put(user_id, vu)
          }
        }
        new_user_vec.toIterator
      }).collect()
      user_vec ++= new_user_vec_array
      val new_user_broadcast = user_vec
      
      // recompute item vectors
      val item_gradient = item_indexed_data.mapPartitions( iterator => {
        val item_vec_gradient = new Array[Array[Double]](nitem+1)
        
        for(i <- 0 until item_vec_gradient.length){
          item_vec_gradient(i) = new Array[Double](dim)
        }
        
        while(iterator.hasNext){
          val entry = iterator.next()
          if(entry._2._2 == true){
            val user_id = entry._2._1
            val item_id = entry._1
            val alt_item_id = Random.nextInt(nitem) + 1
    
            val vu = new_user_broadcast(user_id)
            val vi = item_broadcast(item_id)
            val vj = item_broadcast(alt_item_id)
            
            // compute inner product
            var inner_product = 0.0
            for(i <- 0 until dim){
              inner_product += vu(i) * (vi(i) - vj(i))
            }
            val scalar = 1.0 / (1.0 + math.exp(inner_product))
            
            // gradient descent
            for(i <- 0 until dim){
              item_vec_gradient(item_id)(i) += (scalar * vu(i) - lambda * vi(i))
              item_vec_gradient(alt_item_id)(i) += ( - scalar * vu(i) - lambda * vj(i))
            }
          }
        }
        Array(item_vec_gradient).toIterator
      }).reduce( (v1,v2) => {
        for(i <- 0 until v1.length){
          for(j <- 0 until dim){
            v1(i)(j) += v2(i)(j)
          }
        }
        v1
      })
      // gradient descent
      for(i <- item_vec.keySet){
        val iv = item_vec(i)
        for(j <- 0 until dim){
          iv(j) += beta * item_gradient(i)(j)
        }
      }
      val new_item_broadcast = item_vec
      
      val endTime = (new Date()).getTime
      totalTime += (endTime - startTime).toDouble / 1000
      
      // evaluate loss
      val loss = user_indexed_data.map( entry => {
        if(entry._2._2 == false){
          val user_id = entry._1
          val item_id = entry._2._1
          val alt_item_id = Random.nextInt(nitem) + 1
          
          val vu = new_user_broadcast(user_id)
          val vi = new_item_broadcast(item_id)
          val vj = new_item_broadcast(alt_item_id)
          
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
      }).sum()
      
      println("%5.3f\t%5.8f".format(totalTime, (1-loss/testFreq)))
    }
  }
}