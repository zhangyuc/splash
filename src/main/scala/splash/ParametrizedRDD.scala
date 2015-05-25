package splash

import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import scala.reflect.ClassTag
import java.util.Date
import java.text.SimpleDateFormat

class ParametrizedRDD[T: ClassTag] extends Serializable {
  val aws = new AdaWeightScheduler
  var worksets : rdd.RDD[WorkSet[T]] = _
  var numOfWorkset = 0
  var globalRnd : Random = null
  
  var length : Long = 0
  var totalTimeEllapsed = 0.0
  var proposedGroupNum = 0.0
  
  var process_func :(T, Double, SharedVariableSet, LocalVariableSet) => Any = null
  var evaluate_func : (T, SharedVariableSet, LocalVariableSet) => Double = null
  
  var worksetIterator = 0
  var dataProcessedSinceLastShuffling = 0.0
  var duplicateFactor = 1
  var iterNum = 0
  
  def this(initRdd : rdd.RDD[T], preservePartitioning : Boolean = false){
    this()
    
    val repartitioned_rdd = {
      if(preservePartitioning) initRdd
      else repartitionRDD(initRdd, initRdd.context.defaultParallelism)
    }
    
    globalRnd = new Random
    length = repartitioned_rdd.count()
    numOfWorkset = repartitioned_rdd.partitions.length
    worksets = generateWorkSets(repartitioned_rdd)
  }
  
  private def repartitionRDD[U: ClassTag](init_rdd : rdd.RDD[U], partition_num : Int = 0) = {
    val npar = {
      if(partition_num == 0) init_rdd.partitions.length
      else partition_num
    }
    init_rdd.map( x => (Random.nextInt(npar), x)).partitionBy(new HashPartitioner(npar)).map( x => x._2 )
  }
  
  def duplicateAndReshuffle(duplicateNum : Int) = {
    if(duplicateNum > 0){
      duplicateWorkset(this.worksets, duplicateNum)
      duplicateFactor *= duplicateNum
      length *= duplicateNum
    }
    this
  }
  
  private def duplicateWorkset[U: ClassTag](worksets : rdd.RDD[WorkSet[U]], duplication : Int){
    val dup = duplication
    val reshuffled_data = repartitionRDD(worksets.flatMap(workset => {
      workset.recordArray.iterator
    }).flatMap(x => {
      val array = new Array[Record[U]](dup)
      array(0) = x
      for(i <- 1 until dup){
        array(i) = new Record[U]
        array(i).line = x.line
      }
      array.iterator
    }), numOfWorkset)
    
    val tmp = worksets.zipPartitions(reshuffled_data)( (workset_iter, iter) => {
      val workset = workset_iter.next()
      workset.recordArray = iter.toArray
      workset.length = workset.recordArray.length
      Array(0).iterator
    }).count()
  }
  
  private def generateWorkSets[U: ClassTag] ( init_rdd:rdd.RDD[U] ) = {
    val result = init_rdd.mapPartitionsWithIndex( (index, iter) => {
      val workset = new WorkSet[U]
      workset.id = index
      val tmp_array = new ListBuffer[Record[U]]
      
      while(iter.hasNext){
        val line = iter.next()
        tmp_array.append( {
          val r = new Record[U]
          r.line = line
          r })
        workset.length += 1
      }
      workset.recordArray = tmp_array.toArray
      Array(workset).iterator
    }).cache()
    result
  }
  
  def count() = length
  def getTotalTimeEllapsed() = totalTimeEllapsed
  def getProposedGroupNum() = proposedGroupNum
  def partitions = worksets.partitions
  
  def setProcessFunction(func :(T, Double, SharedVariableSet, LocalVariableSet) => Any){
    process_func = func
  }
  
  def setLossFunction(func: (T, SharedVariableSet, LocalVariableSet) => Double){
    evaluate_func = func
  }
  
  def map[U: ClassTag](func : (T, SharedVariableSet, LocalVariableSet) => U) = {
    val list = new ListBuffer[U]
    val f = func
    worksets.flatMap( workset => {
      for(record <- workset.recordArray){
        val localVar = new LocalVariableSet(record.variable)
        workset.sharedVar.setDelayedDelta(record.delayedDelta)
        list.append(f(record.line, workset.sharedVar, localVar))
        record.variable = localVar.toArray()
        record.delayedDelta = workset.sharedVar.exportDelayedDelta()
      }
      list.iterator
    })
  }
  
  def reduce(func : (T, T) => T) = {
    val f = func
    worksets.flatMap( workset => {
      val orignalCopy = workset.recordArray.map(_.line)
      orignalCopy.iterator
    }).reduce(f)
  }
  
  def foreach(func : (T, SharedVariableSet, LocalVariableSet) => Any) = {
    val f = func
    worksets.foreach( workset => {
      for(record <- workset.recordArray){
        val localVar = new LocalVariableSet(record.variable)
        workset.sharedVar.setDelayedDelta(record.delayedDelta)
        f(record.line, workset.sharedVar, localVar)
        record.variable = localVar.toArray()
        record.delayedDelta = workset.sharedVar.exportDelayedDelta()
      }
    })
  }
  
  def syncSharedVariable(){
    val mergedProp = worksets.context.broadcast( worksets.map( workset => workset.sharedVar.exportProposal(1.0) ).filter( prop => prop != null )
        .reduce( (prop1:Proposal, prop2: Proposal) => prop1 concatinate prop2))
    worksets.foreach( workset => {
      workset.sharedVar.updateByProposal(mergedProp.value, 1)
    } )
    mergedProp.destroy()
  }
  
  def mapSharedVariable[U: ClassTag](func : SharedVariableSet => U) = {
    val f = func
    worksets.map{
      workset => f(workset.sharedVar)
    }
  }
  
  def reduceSharedVariable(func : (SharedVariableSet, SharedVariableSet) => SharedVariableSet) = {
    val f = func
    worksets.map(_.sharedVar).reduce(f)
  }
  
  def foreachSharedVariable( preprocess_func: SharedVariableSet => Any ) {
    val pfunc = preprocess_func
    worksets.foreach( workset => {
      pfunc(workset.sharedVar)
    })
  }
  
  def run(spc: StreamProcessContext){
    if(iterNum == 0 || dataProcessedSinceLastShuffling >= duplicateFactor){
      refresh()
    }
    if(iterNum == 0 && spc.warmStart && spc.threadNum != 1){
      takePass(globalRnd, worksets, process_func, evaluate_func, spc.set("num.of.thread", 1))
    }
    takePass(globalRnd, worksets, process_func, evaluate_func, spc)
    iterNum += 1
  }
  
  def getFirstSharedVariable() = {
    worksets.map( workset => workset.sharedVar ).first()
  }
  
  def getAllSharedVariable() = {
    worksets.map( workset => workset.sharedVar ).collect()
  }
      
  def toRDD () = {
    val processed_data = worksets.flatMap[Record[T]]( workset => {
      val recordList = new ListBuffer[Record[T]]
      recordList ++= workset.recordArray
      recordList.iterator
    })
    processed_data
  }
  
  def unpersist(){
    worksets.unpersist()
  }
  
  def refresh() {
    val shuffle_seed = globalRnd.nextInt(65536)
    worksets.foreach( workset => {
      val rnd = new Random(shuffle_seed + workset.id)
      workset.iterator = 0
      
      // shuffle data points
      val nr = workset.recordArray.length
      for(j <- 0 until nr - 1){
        val randi = rnd.nextInt(nr-j-1)
        val tmp = workset.recordArray(nr-j-1)
        workset.recordArray(nr-j-1) = workset.recordArray(randi)
        workset.recordArray(randi) = tmp
      }
    })
    dataProcessedSinceLastShuffling = 0.0
  }
  
  val getShuffledIndex = (rnd : Random, n:Int) => {
    val array = new Array[Int](n)
    for(i <- 0 until n){
      array(i) = i
    }
    for(j <- 0 until n - 1){
      val randi = rnd.nextInt(n-j-1)
      val tmp = array(n-j-1)
      array(n-j-1) = array(randi)
      array(randi) = tmp
    }
    array
  }
  
  def takePass[U: ClassTag] ( rnd:Random, worksets: rdd.RDD[WorkSet[U]],
    process_func:(U, Double, SharedVariableSet, LocalVariableSet) => Any,
    evaluate_func: (U, SharedVariableSet, LocalVariableSet) => Double, 
    spc : StreamProcessContext ) {
    
    // assign variables
    val beta = 0.9
    val num_of_workset = numOfWorkset
      
    // prepare broadcast
    val func = process_func
    val eval_func = evaluate_func
    val sc = worksets.context
    val process_rand_seed = rnd.nextInt(65536)
    val iterStartTime = (new Date).getTime
    
    // determine reweight
    val thread = {
      if(spc.threadNum == 0){
        worksets.partitions.length
      }
      else{
        spc.threadNum
      }
    }
    val activeSet = sc.broadcast({
      val set = new HashSet[Int]
      for(i <- 0 until thread){
        set.add(worksetIterator + i)
      }
      set
    })
    worksetIterator = (worksetIterator + thread) % num_of_workset
    val stepsize = spc.numOfPassOverLocalData // ratio of data to be processed in each local dataset
    val weight : Int = aws.getAdaWeight(rnd, worksets, process_func, evaluate_func, spc, stepsize, activeSet.value)
    this.proposedGroupNum = weight 
    
    // process data in one split, and in parallel
    worksets.foreach(workset => {
      val batchsize = math.ceil(stepsize * workset.length).toInt
      val threadActive = activeSet.value.contains(workset.id)
      if( threadActive ) // if the thread is active for this iteration
      {
        val sharedVar = workset.sharedVar
        sharedVar.clearDelayedDelta()
        sharedVar.batchSize = batchsize * weight
        sharedVar.delayedAddShrinkFactor = 1.0
        sharedVar.delayedAddDefinedInEarlierIteration = true
        
        for(i <- 0 until batchsize){ 
          if( i + workset.length >= batchsize ){
            sharedVar.delayedAddShrinkFactor = 1.0 / weight
          }
          if( i >= workset.length){
            sharedVar.delayedAddDefinedInEarlierIteration = false
          }
          val record = workset.nextRecord()
          val localVar = new LocalVariableSet(record.variable)
          sharedVar.executeDelayedAdd(record.delayedDelta)
          func(record.line, weight, sharedVar, localVar)
          record.variable = localVar.toArray()
          record.delayedDelta = sharedVar.exportDelayedDelta()
        }
        sharedVar.batchSize = 0
        sharedVar.delayedAddShrinkFactor = 1.0
        sharedVar.delayedAddDefinedInEarlierIteration = true
        
        // construct a proposal
        workset.prop = sharedVar.exportProposal(weight)
        workset.groupID = workset.id % weight
      }
      else{
        workset.prop = null
      }
    })
    
    this.dataProcessedSinceLastShuffling += thread / num_of_workset.toDouble * duplicateFactor * math.min(1.0, stepsize)
    var timeElapsed = ((new Date).getTime - iterStartTime).toDouble / 1000
    
    // merge proposals
    val mergedProp = sc.broadcast(worksets.map( workset => (workset.groupID, workset.prop) ).filter( a => a._2 != null)
        .reduceByKey( (p1:Proposal, p2: Proposal) => p1 concatinate p2 ).reduce( (p1,p2) => (0, p1._2 add p2._2) )
    )
    
    // postprocessing
    worksets.foreach( workset => {
      workset.sharedVar.updateByProposal(mergedProp.value._2, weight)
    } )
    mergedProp.destroy()
    
    // estimate time coefficients
    if(thread > 1) timeElapsed = ((new Date).getTime - iterStartTime).toDouble / 1000
    this.totalTimeEllapsed += timeElapsed
  }
}