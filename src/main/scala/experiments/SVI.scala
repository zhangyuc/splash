package experiments
import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import java.util.Date
import java.text.SimpleDateFormat
import scala.io.Source
import breeze.numerics.digamma
import splash.core._

class DocBatch extends Serializable{
  val docs = new ListBuffer[Doc]
  var testFreq = 0
  var freq = 0
}

class Doc extends Serializable{
  var docID = 0
  var forTrain = true
  var words : collection.Iterable[(Int,Boolean)] = null
}

class SVI {
  val train = (vocfile:String, docfile:String) => {
    val spc = new SplashConf
    spc.maxThreadNum = 64
    spc.autoThread = false
    
    var tmp_minibatch_size = 8
 
    if(vocfile.endsWith("vocab.nips.txt")){
      tmp_minibatch_size = 8
    }
    if(vocfile.endsWith("vocab.enron.txt")){
      tmp_minibatch_size = 24
    }
    if(vocfile.endsWith("vocab.nytimes.txt")){
      tmp_minibatch_size = 128
    }
    
    if(spc.maxThreadNum == 64){
      if(vocfile.endsWith("vocab.nips.txt")){
        spc.dataPerIteration = 1e-8
      }
      if(vocfile.endsWith("vocab.enron.txt")){
        spc.dataPerIteration = 1e-8
      }
      if(vocfile.endsWith("vocab.nytimes.txt")){
        spc.dataPerIteration = 1e-8
      }
    }
    
    val minibatch_size = tmp_minibatch_size
    val num_of_pass = 100000
    val num_of_partition = 64
    val num_of_topic = 20
    val alpha = 50.0 / num_of_topic
    val beta = 0.01
    val conf = new SparkConf().setAppName("LDA-SVI Application").set("spark.driver.maxResultSize", "6G")
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
      var count = 0
      val batchlist = new ListBuffer[DocBatch]
      var currentBatch = new DocBatch
      var isTrain = true
      while(iter.hasNext){
        val doc_wordlist = iter.next()
        val doc = new Doc
        doc.docID = doc_wordlist._1
        doc.words = doc_wordlist._2
        doc.forTrain = isTrain
        isTrain = !isTrain
        
        currentBatch.docs.append(doc)
        currentBatch.freq += doc.words.count( x => true)
        if(!doc.forTrain){
          currentBatch.testFreq += doc.words.count( a => a._2 == false )
        }
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
    val preprocess = (sharedVar: SharedVariableSet) => {
      sharedVar.set("voc_size", voc_size)
      sharedVar.set("num_of_doc", num_of_doc)
      sharedVar.set("num_of_topic", num_of_topic)
      sharedVar.set("num_of_partition", num_of_partition)
      sharedVar.set("alpha",alpha)
      sharedVar.set("beta",beta)
      for(tid <- 0 until num_of_topic){
        sharedVar.declareArray("l:"+tid, voc_size+1)
      }
    }
    val paraRdd = new ParametrizedRDD(data)
    paraRdd.process_func = this.update
    paraRdd.evaluate_func = this.evaluateTrainLoss
    
    paraRdd.foreachSharedVariable(preprocess)
    paraRdd.foreach(initialize)
    paraRdd.syncSharedVariable()

    // take several passes over the dataset
    for(i <- 0 until num_of_pass){
      paraRdd.run(spc)
      if(i % 5 == 0){
        val loss = - paraRdd.map(evaluateTestLoss).reduce( (a,b) => a+b ) / testFreq
        println("%5.3f\t%5.5f\t%d".format(paraRdd.totalTimeEllapsed, loss, paraRdd.lastIterationThreadNumber))
      }
    }
  }
  
  val evaluateTrainLoss = (docBatch:DocBatch, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    val voc_size = sharedVar.get("voc_size")
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    val alpha = sharedVar.get("alpha")
    val beta = sharedVar.get("beta")
    
    // compute gamma and lambda using variational inference
    var loss = 0.0
    var count = 0
    for(entry <- docBatch.docs.filter( doc => doc.forTrain )){
      val doc_id = entry.docID
      for(word_entry <- entry.words){
        val word_id = word_entry._1
        val phi = new Array[Double](num_of_topic)
        var sum = 0.0
        for(tid <- 0 until num_of_topic){
          val t1 = (localVar.get("g:"+doc_id+":"+tid) + alpha) / (localVar.get("g_all:"+doc_id) + alpha * num_of_topic) 
          val t2 = (sharedVar.getArrayElement("l:"+tid, word_id) + beta) / (sharedVar.get("l_all:"+tid) + beta * voc_size)
          sum += t1 * t2
        }
        loss -= math.log(sum)
        count += 1
      }
    }
    loss / count
  }
  
  val evaluateTestLoss = (docBatch:DocBatch, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    val voc_size = sharedVar.get("voc_size")
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    val alpha = sharedVar.get("alpha")
    val beta = sharedVar.get("beta")
    
    // compute gamma and lambda using variational inference
    var loss = 0.0
    for(entry <- docBatch.docs.filter( doc => !doc.forTrain)){
      val doc_id = entry.docID
      
      // training using observed data in test set
      // initialize gamma
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
        
        for(word_entry <- entry.words.filter( w => w._2 == true )){
          val word_id = word_entry._1
          val phi = new Array[Double](num_of_topic)
          var sum = 0.0
          for(tid <- 0 until num_of_topic){
            phi(tid) = math.exp({
              val t1 = digamma(gamma(tid) + alpha) - digamma(gamma_all + alpha * num_of_topic) 
              val t2 = digamma(sharedVar.getArrayElement("l:"+tid, word_id) + beta) - digamma(sharedVar.get("l_all:"+tid) + beta * voc_size)
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
        localVar.set("g:" + doc_id + ":" + tid, gamma(tid))
      }
      localVar.set("g_all:" + doc_id, gamma_all)
      
      // testing using held-out data in test set
      for(word_entry <- entry.words.filter( w => w._2 == false )){
        val word_id = word_entry._1
        val phi = new Array[Double](num_of_topic)
        var sum = 0.0
        for(tid <- 0 until num_of_topic){
          val t1 = (gamma(tid) + alpha) / (gamma_all + alpha * num_of_topic) 
          val t2 = (sharedVar.getArrayElement("l:"+tid, word_id) + beta) / (sharedVar.get("l_all:"+tid) + beta * voc_size)
          sum += t1 * t2
        }
        loss -= math.log(sum)
      }
    }
    loss
  }
  
  val initialize = (docBatch:DocBatch, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    for(entry <- docBatch.docs.filter( doc => doc.forTrain))
    {
      val doc_id = entry.docID
      for(word_entry <- entry.words){
        val word_id = word_entry._1
        val init_topic = Random.nextInt(num_of_topic)
        sharedVar.addArrayElement( "l:" + init_topic, word_id, 1.0)
        sharedVar.add("l_all:" + init_topic, 1.0)
      }
    }
  }
  
  val update = (docBatch:DocBatch, weight : Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
    val t = sharedVar.get("t")
    val voc_size = sharedVar.get("voc_size").toInt
    val num_of_topic = sharedVar.get("num_of_topic").toInt
    val num_of_doc = sharedVar.get("num_of_doc").toInt
    val num_of_partition = sharedVar.get("num_of_partition")
    val alpha = sharedVar.get("alpha")
    val beta = sharedVar.get("beta")
    val lambda = new Array[HashMap[Int,Double]](num_of_topic)
    val num_of_train_doc = docBatch.docs.count( doc => doc.forTrain )
      
    for(tid <- 0 until num_of_topic){
      lambda(tid) = new HashMap[Int,Double]
    }
    
    for(doc <- docBatch.docs.filter(doc => doc.forTrain))
    {
      if(doc.forTrain){
        // initialize gamma
        val doc_id = doc.docID
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
          
          for(word_entry <- doc.words){
            val word_id = word_entry._1
            val phi = new Array[Double](num_of_topic)
            var sum = 0.0
            for(tid <- 0 until num_of_topic){
              phi(tid) = math.exp({
                val t1 = digamma(gamma(tid) + alpha) - digamma(gamma_all + alpha * num_of_topic) 
                val t2 = digamma(sharedVar.getArrayElement("l:"+tid, word_id) + beta) - digamma(sharedVar.get("l_all:"+tid) + beta * voc_size)
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
          localVar.set("g:" + doc_id + ":" + tid, gamma(tid))
        }
        localVar.set("g_all:" + doc_id, gamma_all)
        
        // update lambda
        for(word_entry <- doc.words){
          val word_id = word_entry._1
          val phi = new Array[Double](num_of_topic)
          var sum = 0.0
          for(tid <- 0 until num_of_topic){
            phi(tid) = math.exp({
              val t1 = digamma(gamma(tid) + alpha) - digamma(gamma_all + alpha * num_of_topic) 
              val t2 = digamma(sharedVar.getArrayElement("l:"+tid, word_id) + beta) - digamma(sharedVar.get("l_all:"+tid) + beta * voc_size)
              t1+t2
            })
            sum += phi(tid)
          }
          for(tid <- 0 until num_of_topic){
            lambda(tid).put(word_id, lambda(tid).applyOrElse(word_id, (x:Any)=>0.0) + num_of_doc.toDouble / num_of_train_doc * phi(tid) / sum )
          }
        }
      }
    }
    
    // update lambda as shared variable
    if(num_of_train_doc > 0)
    {
      val nos = weight.toInt
      var shrinkage = 1.0
      for(i <- 0 until nos){
        shrinkage *= 1 - math.pow(1.0 + t + i, - 0.7)
      }
      val stepsize = 1 - shrinkage

      for(tid <- 0 until num_of_topic){
        sharedVar.multiplyArray("l:"+tid, 1 - stepsize)
        sharedVar.multiply("l_all:"+tid, 1 - stepsize)
        var lambda_all_delta = 0.0
        for(word_id <- lambda(tid).keySet){
          val new_lambda = lambda(tid)(word_id)
          sharedVar.addArrayElement("l:"+tid, word_id, stepsize * new_lambda)
          lambda_all_delta += stepsize * new_lambda
        }
        sharedVar.add("l_all:"+tid, lambda_all_delta)
      }
      sharedVar.add("t", nos)
    }
  }
}