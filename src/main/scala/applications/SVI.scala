import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import java.util.Date
import java.text.SimpleDateFormat
import scala.io.Source
import breeze.numerics.digamma
import splash._

class DocBatch extends Serializable{
  val docs = new ListBuffer[(Int, collection.Iterable[(Int, Boolean)])]
  var testFreq = 0
  var freq = 0
}

class SVI {
  val train = (vocfile:String, docfile:String) => {
    val spc = new StreamProcessContext
    spc.threadNum = 64
    spc.useAdaptiveWeight = true
    spc.weight = 64
    spc.adaptiveWeightSampleRatio = 0.1

    val num_of_pass = 1000
    val num_of_partition = 64
    val num_of_topic = 20
    val minibatch_size = 8
    val robustness = 0.0
    val alpha = 50.0 / num_of_topic
    val beta = 0.01
    val conf = new SparkConf().setAppName("LDA-SVI Application").set("spark.driver.maxResultSize", "3G")
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
    }).groupByKey().repartition(num_of_partition).mapPartitions( iter => {
      var count = 0
      val batchlist = new ListBuffer[DocBatch]
      var currentBatch = new DocBatch
      while(iter.hasNext){
        val doc = iter.next()
        currentBatch.docs.append(doc)
        currentBatch.freq += doc._2.count( x => true)
        currentBatch.testFreq += doc._2.count( a => a._2 == false )
        count += 1
        if(count == minibatch_size){
          batchlist.append(currentBatch)
          currentBatch = new DocBatch
          count = 0
        }
      }
      if(currentBatch.docs.length > 0){
        batchlist.append(currentBatch)
      }
      batchlist.iterator
    }).cache()
    
    val num_of_doc = data.map( batch => batch.docs.length ).reduce( (a,b) => a + b )
    val freq = data.map( batch => batch.freq ).reduce( (a,b) => a + b )
    val testFreq = data.map( batch => batch.testFreq ).reduce( (a,b) => a + b )
    
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
    println("Stochastic Variational Inference")
    println("vocabulary size = " + voc_size)
    println("numumber of document = " + num_of_doc)
    println("found " + (freq - testFreq) + " tokens for training and " + testFreq + " tokens for testing.")
    
    // manager start processing data
    val preprocess = (sharedVar:ParameterSet) => {
      sharedVar.set("voc_size", voc_size)
      sharedVar.set("num_of_doc", num_of_doc)
      sharedVar.set("num_of_topic", num_of_topic)
      sharedVar.set("num_of_partition", num_of_partition)
      sharedVar.set("alpha",alpha)
      sharedVar.set("beta",beta)
    }
    val paraRdd = new ParametrizedRDD[DocBatch]( data )
    paraRdd.process_func = this.update
    paraRdd.evaluate_func = this.evaluateTrainLoss
    paraRdd.postprocess_func = this.postprocess
    
    paraRdd.foreachSharedVariable(preprocess)
    paraRdd.foreach(initialize)
    paraRdd.syncSharedVariable()
    val loss = math.exp( paraRdd.map(evaluateTestLoss).reduce( (a,b) => a+b ) / testFreq )
    println("%5.3f\t%5.5f\t%f".format(paraRdd.totalTimeEllapsed, loss, paraRdd.proposedWeight))

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
      if(kv_pair._1.startsWith("l:")){
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
  
  val postprocess = (sharedVar : ParameterSet) => {
    val voc_size = sharedVar.get("voc_size").toInt
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    for(tid <- 0 until num_of_topic){
      var sum = 0.0
      for(word_id <- 1 to voc_size){
        var lambda = sharedVar.get("l:"+tid+":"+word_id)
        if(lambda < 0){
          sharedVar.update("l:"+tid+":"+word_id, -lambda)
          lambda = 0
        }
        sum += lambda
      }
      sharedVar.update("l_all:"+tid, sum - sharedVar.get("l_all:"+tid))
    }
  }
  
  val evaluateTrainLoss = (docBatch:DocBatch, sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    val voc_size = sharedVar.get("voc_size")
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    val alpha = sharedVar.get("alpha")
    val beta = sharedVar.get("beta")
    
    // compute gamma and lambda using variational inference
    var loss = 0.0
    for(entry <- docBatch.docs){
      val doc_id = entry._1
      for(word_entry <- entry._2){
        if(word_entry._2 == true){
          val word_id = word_entry._1
          val phi = new Array[Double](num_of_topic)
          var sum = 0.0
          for(tid <- 0 until num_of_topic){
            val t1 = (localVar.get("g:"+doc_id+":"+tid) + alpha) / (localVar.get("g_all:"+doc_id) + alpha * num_of_topic) 
            val t2 = (sharedVar.get("l:"+tid + ":" + word_id) + beta) / (sharedVar.get("l_all:"+tid) + beta * voc_size)
            sum += t1 * t2
          }
          loss -= math.log(sum)
        }
      }
    }
    loss
  }
  
  val evaluateTestLoss = (docBatch:DocBatch, sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    val voc_size = sharedVar.get("voc_size")
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    val alpha = sharedVar.get("alpha")
    val beta = sharedVar.get("beta")
    
    // compute gamma and lambda using variational inference
    var loss = 0.0
    for(entry <- docBatch.docs){
      val doc_id = entry._1
      for(word_entry <- entry._2){
        if(word_entry._2 == false){
          val word_id = word_entry._1
          val phi = new Array[Double](num_of_topic)
          var sum = 0.0
          for(tid <- 0 until num_of_topic){
            val t1 = (localVar.get("g:"+doc_id+":"+tid) + alpha) / (localVar.get("g_all:"+doc_id) + alpha * num_of_topic) 
            val t2 = (sharedVar.get("l:"+tid + ":" + word_id) + beta) / (sharedVar.get("l_all:"+tid) + beta * voc_size)
            sum += t1 * t2
          }
          loss -= math.log(sum)
        }
      }
    }
    loss
  }
  
  val initialize = (docBatch:DocBatch, sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    for(entry <- docBatch.docs)
    {
      val doc_id = entry._1
      for(word_entry <- entry._2){
        if(word_entry._2 == true){
          val word_id = word_entry._1
          val init_topic = Random.nextInt(num_of_topic)
          sharedVar.update( "l:"+init_topic+":"+word_id, 1.0)
          sharedVar.update( "l_all:" + init_topic, 1.0 )
        }
      }
    }
  }
  
  val update = ( rnd: Random, docBatch:DocBatch, weight : Double, sharedVar : ParameterSet,  localVar: ParameterSet ) => {
    val t = sharedVar.get("t")
    val voc_size = sharedVar.get("voc_size").toInt
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    val num_of_doc = sharedVar.get("num_of_doc").toInt
    val num_of_partition = sharedVar.get("num_of_partition")
    val alpha = sharedVar.get("alpha")
    val beta = sharedVar.get("beta")
    val lambda = new Array[HashMap[Int,Double]](num_of_topic)
    for(tid <- 0 until num_of_topic){
      lambda(tid) = new HashMap[Int,Double]
    }
    
    for(entry <- docBatch.docs)
    {
      // initialize gamma
      val doc_id = entry._1
      val gamma = new Array[Double](num_of_topic)
      var gamma_all = 0.0
      for(tid <- 0 until num_of_topic){
        gamma(tid) = localVar.get("g:"+doc_id+":"+tid)
        gamma_all += gamma(tid)
      }
      
      // recompute gamma until convergence
      var delta_gamma = 1e6
      while(delta_gamma > 0.01){
        val new_gamma = new Array[Double](num_of_topic)
        var new_gamma_all = 0.0
        
        for(word_entry <- entry._2){
          if(word_entry._2 == true){
            val word_id = word_entry._1
            val phi = new Array[Double](num_of_topic)
            var sum = 0.0
            for(tid <- 0 until num_of_topic){
              phi(tid) = math.exp({
                val t1 = digamma(gamma(tid) + alpha) - digamma(gamma_all + alpha * num_of_topic) 
                val t2 = digamma(sharedVar.get("l:"+tid + ":" + word_id) + beta) - digamma(sharedVar.get("l_all:"+tid) + beta * voc_size)
                t1+t2
              })
              sum += phi(tid)
            }
            for(tid <- 0 until num_of_topic){
              new_gamma(tid) += phi(tid) / sum
              new_gamma_all += phi(tid) / sum
            }
          }
        }
        
        // measure the change of gamma
        delta_gamma = 0
        for(tid <- 0 until num_of_topic){
          delta_gamma += (new_gamma(tid) - gamma(tid)).abs
          gamma(tid) = new_gamma(tid)
          gamma_all = new_gamma_all
        }
      }
      
      // update gamma
      for(tid <- 0 until num_of_topic){
        localVar.set("g:" + doc_id + ":" + tid, gamma(tid))
      }
      localVar.set("g_all:" + doc_id, gamma_all)
      
      // update lambda
      for(word_entry <- entry._2){
        if(word_entry._2 == true){
          val word_id = word_entry._1
          val phi = new Array[Double](num_of_topic)
          var sum = 0.0
          for(tid <- 0 until num_of_topic){
            phi(tid) = math.exp({
              val t1 = digamma(gamma(tid) + alpha) - digamma(gamma_all + alpha * num_of_topic) 
              val t2 = digamma(sharedVar.get("l:"+tid + ":" + word_id) + beta) - digamma(sharedVar.get("l_all:"+tid) + beta * voc_size)
              t1+t2
            })
            sum += phi(tid)
          }
          for(tid <- 0 until num_of_topic){
            lambda(tid).put(word_id, lambda(tid).applyOrElse(word_id, (x:Any)=>0.0) + num_of_doc.toDouble / docBatch.docs.length * phi(tid) / sum )
          }
        }
      }
    }
    
    // update lambda as shared variable
    val nos = 1.0
    val stepsize = math.min(1.0, nos * math.pow(64.0 + t, - 0.7))
    
    for(tid <- 0 until num_of_topic){
      var lambda_all_delta = 0.0
      for(word_id <- 1 to voc_size){
        val old_lambda = sharedVar.get("l:"+tid+":"+word_id)
        val new_lambda = lambda(tid).applyOrElse(word_id, (x:Any)=>0.0)
        sharedVar.update("l:"+tid+":"+word_id, stepsize * (new_lambda - old_lambda))
        lambda_all_delta += stepsize * (new_lambda - old_lambda)
      }
      val old_lambda_all = sharedVar.get("l_all:"+tid)
      sharedVar.update("l_all:"+tid, lambda_all_delta)
    }
    sharedVar.update("t", nos)
  }
}