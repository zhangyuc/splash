package splash.sampling
import scala.util.Random
import org.apache.spark.mllib.linalg.{Vectors,Vector,DenseVector,SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import splash.core._

class WordToken(initWordId : Int, initWordCount : Int, initTopicId : Int) extends Serializable {
  var wordId = initWordId
  var wordCount = initWordCount
  var topicId = initTopicId 
}

class CollapsedGibbsSamplingForLDA {
  var iters = 10
  var process : ((Int, WordToken), Double, SharedVariableSet, LocalVariableSet ) => Unit = null
  var evalLoss : ((Int, WordToken), SharedVariableSet, LocalVariableSet ) => Double = null
  var printDebugInfo = false
  
  // LDA parameters
  var alpha = 0.0
  var beta = 0.0
  var numTopics = 0
  var numDocuments = 0
  var vocabSize = 0
  
  def sample(data: RDD[(Int, WordToken)]) = {
    val numPartitions = data.partitions.length
    val n = data.map( x => x._2.wordCount ).sum()
    
    val numTopics = this.numTopics
    val tmpPair = data.map( x => (x._1, x._2.wordId) ).reduce((pair1, pair2) => (math.max(pair1._1, pair2._1), math.max(pair1._2, pair2._2)))
    val numDocuments = tmpPair._1 + 1
    val vocabSize = tmpPair._2 + 1
    this.numDocuments = numDocuments
    this.vocabSize = vocabSize
    
    val dtLength = numDocuments * numTopics + numDocuments
    val wtLength = vocabSize * numTopics + numTopics
    if(printDebugInfo){
      println("numDocuments = " + numDocuments)
      println("vocabSize = " + vocabSize)
      println("n = " + n)
    }

    val paramRdd = new ParametrizedRDD(data.partitionBy(new HashPartitioner(numPartitions)))
    setProcessFunction()
    setEvalFunction()
    
    paramRdd.foreachSharedVariable( sharedVar => {
      for(wordId <- 0 until vocabSize) sharedVar.declareArray("w"+wordId, numTopics)
      for(docId <- 0 until numDocuments) sharedVar.declareArray("d"+docId, numTopics)
      sharedVar.declareArray("ws", numTopics)
      sharedVar.declareArray("ds", numDocuments)
    })
    
    paramRdd.foreach((record, sharedVar,  localVar) => {
      val docId = record._1
      val wordId = record._2.wordId
      val topicId = record._2.topicId
      val weight = record._2.wordCount
      
      // update shared variables
      sharedVar.addArrayElement( "w"+wordId, topicId, weight)
      sharedVar.addArrayElement( "d"+docId, topicId, weight)
      sharedVar.addArrayElement( "ws", topicId, weight)
      sharedVar.addArrayElement( "ds", docId, weight)
      sharedVar.dontSyncArray("d"+docId)
      
      // delayed update
      sharedVar.delayedAddArrayElement( "w"+wordId, topicId, -weight)
      sharedVar.delayedAddArrayElement( "d"+docId, topicId, -weight)
      sharedVar.delayedAddArrayElement( "ws", topicId, -weight)
    })
    paramRdd.syncSharedVariable()
    
    paramRdd.process_func = this.process
    paramRdd.evaluate_func = this.evalLoss
    
    val spc = (new SplashConf).set("auto.thread", false)
    for( i <- 0 until this.iters ){
      paramRdd.run(spc)
      if(printDebugInfo){
        val loss = math.exp( paramRdd.map(evalLoss).sum() / n )
        println("%5.3f\t%5.8f\t".format(paramRdd.totalTimeEllapsed, loss) + paramRdd.lastIterationThreadNumber)
      }
    }
    paramRdd.map((record,svar,lvar) => record)
  }
  
  private def setProcessFunction(){    
    val alpha = this.alpha
    val beta = this.beta
    val numTopics = this.numTopics
    val numDocument = this.numDocuments
    val vocabSize = this.vocabSize    
    this.process = (record: (Int, WordToken), weight: Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
      val docId = record._1
      val wordId = record._2.wordId
      val count = record._2.wordCount * weight
      
      // calculate the probability that this word belongs to some topic
      val wtValue = sharedVar.getArray("w"+wordId)
      val dtValues = sharedVar.getArray("d"+docId)
      val wtSumValues = sharedVar.getArray("ws")
      
      var sumProb = 0.0
      val prob = new Array[Double](numTopics)
      for(tid <- 0 until numTopics){
        prob(tid) = (dtValues(tid) + alpha) * (wtValue(tid) + beta) / (wtSumValues(tid) + beta * vocabSize)
        sumProb += prob(tid)
      }
      for(tid <- 0 until numTopics){
        prob(tid) /= sumProb
      }
      
      // sample the new topic for this word
      val rand = Random.nextDouble()
      var topicId = 0
      var accuProb = prob(0)
      while( rand >= accuProb){
        topicId += 1
        accuProb += prob(topicId)
      }
      record._2.topicId = topicId
      
      // update shared variables
      sharedVar.addArrayElement( "w"+wordId, topicId, count)
      sharedVar.addArrayElement( "ws", topicId, count)
      sharedVar.addArrayElement( "d"+docId, topicId, count)
      sharedVar.dontSyncArray("d"+docId)
      
      // delayed update
      sharedVar.delayedAddArrayElement( "w"+wordId, topicId, -count)
      sharedVar.delayedAddArrayElement( "ws", topicId, -count)
      sharedVar.delayedAddArrayElement( "d"+docId, topicId, -count)
    }
  }
  
  private def setEvalFunction(){
    val alpha = this.alpha
    val beta = this.beta
    val numTopics = this.numTopics
    val numDocument = this.numDocuments
    val vocabSize = this.vocabSize
    
    this.evalLoss = (record: (Int, WordToken), sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
      val docId = record._1
      val wordId = record._2.wordId
      val count = record._2.wordCount
      
      // calculate the probability that this word belongs to some topic
      val wtValue = sharedVar.getArray("w"+wordId)
      val dtValues = sharedVar.getArray("d"+docId)
      val wtSumValues = sharedVar.getArray("ws")
      val dtSum = sharedVar.getArrayElement("ds", docId)
      
      var sumProb = 0.0
      val prob = new Array[Double](numTopics)
      for(tid <- 0 until numTopics){
        prob(tid) = (dtValues(tid) + alpha) / (dtSum + alpha * numTopics) * (wtValue(tid) + beta) / (wtSumValues(tid) + beta * vocabSize)
        sumProb += prob(tid)
      }
      -math.log(sumProb)*count
    }
  }
  
  def setAlphaBeta(abPair : (Double,Double)) = {
    this.alpha = abPair._1
    this.beta = abPair._2
    this
  }
  
  def setNumTopics(numTopics: Int) = {
    this.numTopics = numTopics 
    this 
  }
  
  def setNumIterations(iters: Int) = {
    this.iters = iters 
    this 
  }
  
  def setPrintDebugInfo(printDebugInfo : Boolean) = {
    this.printDebugInfo = printDebugInfo
    this
  }
}
