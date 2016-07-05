package splash.clustering
import scala.util.Random
import org.apache.spark.mllib.linalg.{Matrix,Matrices,Vectors,Vector,DenseVector,SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import breeze.numerics.digamma
import splash.core._
import scalaxy.loops._
import scala.language.postfixOps

class WordToken(initWordId : Int, initWordCount : Int, initTopicId : Array[Int] = null) extends Serializable {
  var wordId = initWordId
  var wordCount = initWordCount
  var topicId = initTopicId
}

/*
 * This class implements the Collapsed Gibbs Sampling algorithm for fitting the Latent Dirichlet Allocation
 * (LDA) model. To use this package, the dataset should be an instance of RDD[(docId, Array[wordToken])]. The docId
 * is the ID of the document, the wordToken represents a word token in the document.
 *
 * docId, wordId and topicId should be integers starting from zero. The Collapsed Gibbs Sampling algorithm
 * resamples the topicId for each word.
 */

class LDA {
  var iters = 10
  var process : ((Int, Array[WordToken]), Double, SharedVariableSet, LocalVariableSet ) => Unit = (_, _, _, _) => ()
  var evalLoss : ((Int, Array[WordToken]), SharedVariableSet, LocalVariableSet ) => Double = null
  var printDebugInfo = false
  var maxThreadNum = 0
  var topicsMatrix : Array[Array[Double]] = null

  // LDA parameters
  var alpha = 0.0
  var beta = 0.0
  var numTopics = 0
  var numDocuments = 0
  var vocabSize = 0

  /*
   * The sample method returns an RDD[(docId, wordToken)] object in which the
   * topic of each word token has been resampled.
   */
  def train(data: RDD[(Int, Array[WordToken])]) = {
    val numPartitions = data.partitions.length
    var d_train = data.map( pair => if (pair._1 >= 0) 1 else 0 ).sum()
    var d_test = data.count - d_train
    var n_test = data.map( pair => {var sum = 0; for(token <- pair._2) if(pair._1 < 0 && (pair._1 + token.wordId).hashCode() % 10 == 0) sum += token.wordCount; sum} ).sum()
    val vocabSize = data.map( pair => { var max = 0; for( token <- pair._2 ) max = math.max(max, token.wordId); max } ).max() + 1
    val numDocuments = data.map( pair => pair._1 ).max() + 1

    val numTopics = this.numTopics
    this.numDocuments = numDocuments
    this.vocabSize = vocabSize

    val dtLength = numDocuments * numTopics + numDocuments
    val wtLength = vocabSize * numTopics + numTopics
    if(printDebugInfo){
      println("numDocuments = " + numDocuments)
      println("vocabSize = " + vocabSize)
      println("document for training = " + d_train)
      println("document for testing = " + d_test)
    }

    val paramRdd = new ParametrizedRDD(data)
    setProcessFunction()
    setEvalFunction()

    // initialization
    val alpha = this.alpha
    val beta = this.beta
    paramRdd.foreachSharedVariable( sharedVar => {
      for(wordId <- 0 until vocabSize){
        sharedVar.setArray("w"+wordId, Array.fill(numTopics)(beta))
      }
      sharedVar.setArray("ws", Array.fill(numTopics)(beta * vocabSize))
    })

    paramRdd.foreach((doc, sharedVar,  localVar) => {
      val docId = doc._1
      if(docId >= 0)
      {
        for(token <- doc._2){
          val wordId = token.wordId
          var topicId = token.topicId
          val weight = token.wordCount
          val subweight = token.wordCount.toDouble / topicId.length

          // update shared variables
          for(id <- topicId){
            sharedVar.addArrayElement( "w"+wordId, id, subweight)
            sharedVar.addArrayElement( "ws", id, subweight)
          }
        }
      }
      else{
        // DO NOTHING FOR TEST DOCUMENT
      }
    })
    paramRdd.syncSharedVariable()
    paramRdd.process_func = this.process
    paramRdd.evaluate_func = this.evalLoss

    val spc = (new SplashConf).set("auto.thread", false).set("max.thread.num", this.maxThreadNum)
    for( i <- 0 until this.iters){
      paramRdd.run(spc)
      if(printDebugInfo){
        val loss = - paramRdd.map(evalLoss).sum() / n_test
        println("%5.3f\t%5.8f".format(paramRdd.totalTimeEllapsed, loss))
      }
    }

    val sharedVar = paramRdd.getSharedVariable();
    val wordSumCount = sharedVar.getArray("ws");
    topicsMatrix = new Array[Array[Double]](vocabSize)
    for(wordId <- 0 until vocabSize){
      topicsMatrix(wordId) = sharedVar.getArray("w"+wordId)
      for(tid <- 0 until numTopics optimized){
        topicsMatrix(wordId)(tid) /= wordSumCount(tid)
      }
    }
    paramRdd.map( (record,svar,lvar) => record )
  }

  private def setProcessFunction(): Unit = {
    val alpha = this.alpha
    val beta = this.beta
    val numTopics = this.numTopics
    val numDocument = this.numDocuments
    val vocabSize = this.vocabSize
    this.process = (doc: (Int, Array[WordToken]), weight: Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
      val docId = doc._1
      val tokenArray = doc._2

      if(docId >= 0){
        // reshuffle the document
        var length = tokenArray.length
        for(i <- 0 until (length - 1) optimized){
          val randi = Random.nextInt(length - i)
          val tmp = tokenArray(length - i - 1)
          tokenArray(length - i - 1) = tokenArray(randi)
          tokenArray(randi) = tmp
        }

        // construct document-topic table
        val dtValues = Array.fill(numTopics)(alpha)
        for(token <- tokenArray){
          val subweight = token.wordCount.toDouble / token.topicId.length
          for(id <- token.topicId) dtValues(id) += subweight
        }

        // sample new topics for the document
        for(i <- 0 until tokenArray.length){
          val token = tokenArray(i)
          val wordId = token.wordId

          // execute delayed update
          val subweight = token.wordCount.toDouble / token.topicId.length
          for(id <- token.topicId){
            if(id >= 0){
              dtValues(id) -= subweight
              sharedVar.executeDelayedAddArrayElement( "w"+wordId, id, -subweight)
              sharedVar.executeDelayedAddArrayElement( "ws", id, -subweight)
            }
          }

          // calculate the probability distribution for the sampling
          val wtValue = sharedVar.getArray("w"+wordId)
          val wtSumValues = sharedVar.getArray("ws")

          var sumProb = 0.0
          val prob = new Array[Double](numTopics)
          for(tid <- 0 until numTopics optimized){
            val p = dtValues(tid) * wtValue(tid) / wtSumValues(tid)
            prob(tid) = p
            sumProb += p
          }

          // sample new topics for this word
          val subcount = token.wordCount.toDouble / token.topicId.length * weight
          for(i <- 0 until token.topicId.length){
            val rand = Random.nextDouble() * sumProb
            var topicId = 0
            var accuProb = prob(0)
            while( rand >= accuProb){
              topicId += 1
              accuProb += prob(topicId)
            }

            // update local variables
            token.topicId(i) = topicId
            dtValues(topicId) += subcount

            // update shared variables
            sharedVar.addArrayElement( "w"+wordId, topicId, subcount)
            sharedVar.addArrayElement( "ws", topicId, subcount)
          }
        }
      }
      else{
        // DO NOTHING FOR TEST DOCUMENT
      }
    }
  }

  private def setEvalFunction(): Unit = {
    val alpha = this.alpha
    val beta = this.beta
    val numTopics = this.numTopics
    val numDocument = this.numDocuments
    val vocabSize = this.vocabSize

    this.evalLoss = (doc: (Int, Array[WordToken]), sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
      val docId = doc._1

      if(docId < 0){
        val tokenArray = doc._2
        val length = tokenArray.length
        var loss = 0.0
        val isObserved = (tk : WordToken) => ((docId + tk.wordId).hashCode() % 10 != 0)

        // construct word-topic table
        val wtValue = new Array[Array[Double]](length)
        for(i <- 0 until length optimized){
          wtValue(i) = sharedVar.getArray("w"+tokenArray(i).wordId)
        }
        val wtSumValues = sharedVar.getArray("ws")

        val betaSum = new Array[Double](numTopics)
        for(tid <- 0 until numTopics) { betaSum(tid) = math.exp(digamma(wtSumValues(tid))) }
        val beta = new Array[Array[Double]](length)
        for(i <- 0 until length){
          beta(i) = new Array[Double](numTopics)
          for(tid <- 0 until numTopics) beta(i)(tid) = math.exp(digamma(wtValue(i)(tid))) / betaSum(tid)
        }

        // construct document-topic table
        var gamma = Array.fill(numTopics)(alpha)
        var gammaSum = alpha * numTopics
        for(token <- tokenArray){
          if(isObserved(token)){
            gammaSum += token.wordCount.toDouble
            for(tid <- 0 until numTopics optimized){
              gamma(tid) += token.wordCount.toDouble / numTopics
            }
          }
        }

        // recompute gamma until convergence
        for(iter <- 0 until 10){
          val new_gamma = Array.fill(numTopics)(alpha)
          val digamma_gamma = new Array[Double](numTopics)
          for(tid <- 0 until numTopics) { digamma_gamma(tid) = math.exp(digamma(gamma(tid))) }

          for(i <- 0 until tokenArray.length optimized){
            val token = tokenArray(i)
            if(isObserved(token)){
              val phi = new Array[Double](numTopics)
              var sum = 0.0
              var tid = 0
              while(tid < numTopics){
                phi(tid) = beta(i)(tid) * digamma_gamma(tid)
                sum += phi(tid)
                tid += 1
              }
              val scale = token.wordCount / sum
              tid = 0
              while(tid < numTopics){
                new_gamma(tid) += phi(tid) * scale
                tid += 1
              }
            }
          }
          gamma = new_gamma
        }

        // compute log-likelihood on test data
        for(i <- 0 until length){
          val token = tokenArray(i)
          if(!isObserved(token)){
            var sumProb = 0.0
            val prob = new Array[Double](numTopics)
            for(tid <- 0 until numTopics optimized){
              prob(tid) = gamma(tid) * wtValue(i)(tid) / (gammaSum * wtSumValues(tid))
              sumProb += prob(tid)
            }
            loss += -math.log(sumProb) * token.wordCount
          }
        }
        loss
      }
      else{
        // DO NOTHING FOR TRAINING DOCUMENT
        0.0
      }
    }
  }

  /*
   * set the maximum number of threads to run. The default value is equal to the number of Parametrized RDD partitions.
   */
  def setMaxThreadNum(maxThreadNum: Int) = {
    this.maxThreadNum = maxThreadNum
    this
  }

  /*
   * set the number of topics of the LDA model.
   */
  def setAlphaBeta(abPair : (Double,Double)) = {
    this.alpha = abPair._1
    this.beta = abPair._2
    this
  }

  /*
   * set the (alpha, beta) hyper-parameters of the LDA model.
   */
  def setNumTopics(numTopics: Int) = {
    this.numTopics = numTopics
    this
  }

  /*
   * set the number of rounds that SGD runs and synchronizes.
   */
  def setNumIterations(iters: Int) = {
    this.iters = iters
    this
  }

  /*
   * set if printing the debug info. The default value is false.
   */
  def setPrintDebugInfo(printDebugInfo : Boolean) = {
    this.printDebugInfo = printDebugInfo
    this
  }
}
