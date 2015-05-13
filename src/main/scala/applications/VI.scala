import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import java.util.Date
import java.text.SimpleDateFormat
import scala.io.Source
import breeze.numerics.digamma

class VI {
  val train = (vocfile:String, docfile:String) => {
    val num_of_pass = 1000
    val num_of_partition = 64
    val conf = new SparkConf().setAppName("LDA-VI Application").set("spark.driver.maxResultSize", "3G")
    val sc = new SparkContext(conf)
    
    // read data and repartition
    val data = sc.textFile(docfile).flatMap( a => {
      val tokens = a.split(" ")
      val list = new ListBuffer[(Int,(Int,Boolean))]
      if(tokens.length == 3){
        for(i <- 0 until tokens(2).toInt){
          list.append((tokens(0).toInt, (tokens(1).toInt, (Random.nextDouble() < 0.5))))
        }
      }
      list.iterator
    }).groupByKey().repartition(num_of_partition).mapPartitions( iter => {
      val doclist = new ListBuffer[Doc]
      var isTrain = true
      while(iter.hasNext){
        val doc_wordlist = iter.next()
        val doc = new Doc
        doc.docID = doc_wordlist._1
        doc.words = doc_wordlist._2
        doc.forTrain = isTrain
        doclist.append(doc)
        isTrain = !isTrain
      }
      doclist.iterator
    }).cache()
    
    val num_of_doc_raw = data.count()
    val freq_raw = data.map( doc => doc.words.count( x => true) ).reduce( (a,b) => a + b )
    val testFreq_raw = data.map( doc => {
      if(doc.forTrain){
        0
      }
      else{
        doc.words.count( a => a._2 == false )
      }
    } ).reduce( (a,b) => a + b )
    
    // read vocabulary
    val vocabulary = new HashMap[Int, String]
    val voc_array = sc.textFile(vocfile).collect()
    var index = 1
    for(line <- voc_array){
      vocabulary.put(index, line)
      index += 1
    }
    val voc_size_raw = vocabulary.count( a => true)
    
    // print basic information
    println("Batch Variational Inference")
    println("vocabulary size = " + voc_size_raw)
    println("numumber of document = " + num_of_doc_raw)
    println("found " + (freq_raw - testFreq_raw) + " tokens for training and " + testFreq_raw + " tokens for testing.")
    
    // initialization
    val num_of_topic = 20
    var parameter = data.mapPartitions( iterator => {
      val freq = new HashMap[String, Double]
      for(entry <- iterator.toList.filter( doc => doc.forTrain ))
      {
        val doc_id = entry.docID
        for(word_entry <- entry.words){
          val word_id = word_entry._1
          val init_topic = Random.nextInt(num_of_topic)
          freq.put( "l:"+init_topic+":"+word_id, freq.applyOrElse("l:"+init_topic+":"+word_id, (x:Any)=>0.0) + 1.0 )
          freq.put( "l_all:" + init_topic, freq.applyOrElse("l_all:" + init_topic, (x:Any)=>0.0) + 1.0 )
        }
      }
      Array(freq).iterator
    } ).reduce( (f1,f2) => {
      for(key <- f2.keySet){
        if(f1.contains(key)){
          f1.put(key, f1(key) + f2(key))
        }
        else{
          f1.put(key, f2(key))
        }
      }
      f1
    } )
    var test_gamma = new HashMap[String, Double]
    
    var totalTime = 0.0
    for(iter <- 0 until num_of_pass){
      val num_of_topic = 20
      val robustness = 0.0
      val alpha = 50.0 / num_of_topic
      val beta = 0.01
      val voc_size = voc_size_raw
      val parameter_broadcast = parameter
      
      val startTime = (new Date()).getTime
      ///////////////////////////////////
      // recompute gamma and lambda
      ///////////////////////////////////
      parameter = data.mapPartitions( iterator => {
        val new_parameter = new HashMap[String, Double]
        val train_docs = iterator.toList.filter( doc => doc.forTrain )
        for(doc <- train_docs)
        {
          // initialize gamma
          val doc_id = doc.docID
          val gamma = new Array[Double](num_of_topic)
          var gamma_all = 0.0
          for(tid <- 0 until num_of_topic){
            gamma(tid) = parameter_broadcast.applyOrElse("g:"+doc_id+":"+tid, (x:Any)=>0.0)
            gamma_all += gamma(tid)
          }
          
          // recompute gamma until convergence
          var delta_gamma = 1e6
          while(delta_gamma > 0.01){
            val new_gamma = new Array[Double](num_of_topic)
            var new_gamma_all = 0.0
            
            for(word_entry <- doc.words){
              val word_id = word_entry._1
              val phi = new Array[Double](num_of_topic)
              var sum = 0.0
              for(tid <- 0 until num_of_topic){
                phi(tid) = math.exp({
                  val t1 = digamma(gamma(tid) + alpha) - digamma(gamma_all + alpha * num_of_topic) 
                  val t2 = digamma(parameter_broadcast.applyOrElse("l:"+tid + ":" + word_id, (x:Any)=>0.0) + beta) - digamma(parameter_broadcast.applyOrElse("l_all:"+tid, (x:Any)=>0.0) + beta * voc_size)
                  t1+t2
                })
                sum += phi(tid)
              }
              for(tid <- 0 until num_of_topic){
                new_gamma(tid) += phi(tid) / sum
                new_gamma_all += phi(tid) / sum
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
            new_parameter.put("g:" + doc_id + ":" + tid, gamma(tid))
          }
          new_parameter.put("g_all:" + doc_id, gamma_all)
          
          // update lambda
          for(word_entry <- doc.words){
            val word_id = word_entry._1
            val phi = new Array[Double](num_of_topic)
            var sum = 0.0
            for(tid <- 0 until num_of_topic){
              phi(tid) = math.exp({
                val t1 = digamma(gamma(tid) + alpha) - digamma(gamma_all + alpha * num_of_topic) 
                val t2 = digamma(parameter_broadcast.applyOrElse("l:"+tid + ":" + word_id, (x:Any)=>0.0) + beta) - digamma(parameter_broadcast.applyOrElse("l_all:"+tid, (x:Any)=>0.0) + beta * voc_size)
                t1+t2
              })
              sum += phi(tid)
            }
            for(tid <- 0 until num_of_topic){
              val key = "l:"+tid + ":" + word_id
              val key_all = "l_all:"+tid
              new_parameter.put(key, new_parameter.applyOrElse(key, (x:Any)=>0.0) + phi(tid) / sum )
              new_parameter.put(key_all, new_parameter.applyOrElse(key_all, (x:Any)=>0.0) + phi(tid) / sum )
            }
          }
        }
        // return recomputed gamma
        Array(new_parameter).iterator
      }).reduce( (f1,f2) => {
        for(key <- f2.keySet){
          if(f1.contains(key)){
            f1.put(key, f1(key) + f2(key))
          }
          else{
            f1.put(key, f2(key))
          }
        }
        f1
      } )
      val new_parameter_broadcast = sc.broadcast(parameter)
      val endTime = (new Date()).getTime
      totalTime += (endTime - startTime).toDouble / 1000
      
      ///////////////////////////////////
      // Evaluation
      ///////////////////////////////////
      val test_gamma_broadcast = test_gamma
      val test_pair = data.map( doc => {
        var loss = 0.0
        val new_gamma = new HashMap[String, Double]
        if(!doc.forTrain)
        {
          val doc_id = doc.docID
          // training using observed data in test set
          // initialize gamma
          val gamma = new Array[Double](num_of_topic)
          var gamma_all = 0.0
          for(tid <- 0 until num_of_topic){
            gamma(tid) = test_gamma_broadcast.applyOrElse("g:"+doc_id+":"+tid, (x:Any)=>0.0)
            gamma_all += gamma(tid)
          }
          
          // recompute gamma until convergence
          var delta_gamma = 1e6
          while(delta_gamma > 0.01){
            val new_gamma = new Array[Double](num_of_topic)
            var new_gamma_all = 0.0
            
            for(word_entry <- doc.words.filter( w => w._2 == true )){
              val word_id = word_entry._1
              val phi = new Array[Double](num_of_topic)
              var sum = 0.0
              for(tid <- 0 until num_of_topic){
                phi(tid) = math.exp({
                  val t1 = digamma(gamma(tid) + alpha) - digamma(gamma_all + alpha * num_of_topic) 
                  val t2 = digamma(new_parameter_broadcast.value.applyOrElse("l:"+tid + ":" + word_id, (x:Any)=>0.0) + beta) - digamma(new_parameter_broadcast.value.applyOrElse("l_all:"+tid, (x:Any)=>0.0) + beta * voc_size)
                  t1+t2
                })
                sum += phi(tid)
              }
              for(tid <- 0 until num_of_topic){
                new_gamma(tid) += phi(tid) / sum
                new_gamma_all += phi(tid) / sum
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
            new_gamma.put("g:" + doc_id + ":" + tid, gamma(tid))
          }
          new_gamma.put("g_all:" + doc_id, gamma_all)
          
          // testing using held-out data in test set
          for(word_entry <- doc.words.filter( w => w._2 == false )){
            val word_id = word_entry._1
            val phi = new Array[Double](num_of_topic)
            var sum = 0.0
            for(tid <- 0 until num_of_topic){
              val t1 = (gamma(tid) + alpha) / (gamma_all + alpha * num_of_topic) 
              val t2 = (new_parameter_broadcast.value.applyOrElse("l:"+tid + ":" + word_id, (x:Any)=>0.0) + beta) / (new_parameter_broadcast.value.applyOrElse("l_all:"+tid, (x:Any)=>0.0) + beta * voc_size)
              sum += t1 * t2
            }
            loss -= math.log(sum)
          }
        }
        (new_gamma, loss)
      }).reduce( (f1,f2) => (f1._1 ++ f2._1, f1._2 + f2._2) )
      test_gamma = test_pair._1
      val loss = test_pair._2 / testFreq_raw
      println("%5.3f\t%5.5f".format(totalTime, -loss))
      
      // iteration finish //
    }
  }
}