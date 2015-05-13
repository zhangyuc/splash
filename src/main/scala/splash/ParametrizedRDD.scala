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
  var streamPartitioner : Partitioner = null
  
  var length : Long = 0
  var totalTimeEllapsed = 0.0
  var proposedGroupNum = 0.0
  
  var process_func :(T, Double, SharedVariableSet, LocalVariableSet) => Any = null
  var evaluate_func : (T, SharedVariableSet, LocalVariableSet) => Double = null
  var worksetIterator = 0
  
  var iterNum = 0
  
  def this( initRdd : rdd.RDD[T], preservePartitioning : Boolean = false ){
    this()
    val repartitioned_rdd = {
      if(preservePartitioning){
        initRdd
      }
      else{
        initRdd.repartition(initRdd.context.defaultParallelism)
      }
    }
    length = repartitioned_rdd.count()
    numOfWorkset = repartitioned_rdd.partitions.length
    globalRnd = new Random
    worksets = generateWorkSets(repartitioned_rdd)
  }
  
  def getTotalTimeEllapsed = totalTimeEllapsed
  def getProposedGroupNum = proposedGroupNum
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
      workset.sharedVar.batchSize = workset.length
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
  
  def foreach(func : (T, SharedVariableSet, LocalVariableSet) => Any) = {
    val f = func
    worksets.foreach( workset => {
      workset.sharedVar.batchSize = workset.length
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
  
  def foreachSharedVariable( preprocess_func: SharedVariableSet => Any ) {
    val pfunc = preprocess_func
    worksets.foreach( workset => {
      pfunc(workset.sharedVar)
    })
  }
  
  def run(spc: StreamProcessContext){
    refresh()
    if(iterNum == 0 && spc.warmStart && spc.threadNum != 1){
      takePass(globalRnd, worksets, process_func, evaluate_func, spc.set("num.of.thread", "1"))
      takePass(globalRnd, worksets, process_func, evaluate_func, spc)
    }
    else{
      takePass(globalRnd, worksets, process_func, evaluate_func, spc)
    }
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
  
  def repartition[U: ClassTag](worksets : rdd.RDD[WorkSet[U]]){
    val reshuffled_data = worksets.flatMap(workset => {
      val data_list = new ListBuffer[Record[U]]
      for(record <- workset.recordArray){
        data_list.append(record)
      }
      data_list.iterator
    }).repartition(numOfWorkset)
    
    val tmp = worksets.zipPartitions(reshuffled_data)((workset_iter, iter) => {
      val workset = workset_iter.next()
      workset.recordArray = iter.toArray
      workset.length = workset.recordArray.length
      Array(0).iterator
    }).count()
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
  }
  
  private def generateWorkSets[U: ClassTag] ( single_row_rdd:rdd.RDD[U] ) = {
    val partitioner = streamPartitioner
    
    val result = single_row_rdd.mapPartitionsWithIndex( (index, iter) => {
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
  
  val printTime = () => {
    val fmt = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    println(fmt.format(new Date))
  }
  
  def takePass[U: ClassTag] ( rnd:Random, worksets: rdd.RDD[WorkSet[U]],
    process_func:(U, Double, SharedVariableSet, LocalVariableSet) => Any,
    evaluate_func: (U, SharedVariableSet, LocalVariableSet) => Double, 
    spc : StreamProcessContext ) {
    
    // assign variables
    val beta = 0.9
    val num_of_workset = numOfWorkset
      
    // prepare broadcast
    val blockIteratorBroadcast = worksetIterator
    if(spc.threadNum == 1){ 
      worksetIterator = (worksetIterator + 1) % num_of_workset 
    }
    val func = process_func
    val eval_func = evaluate_func
    val sc = worksets.context
    val priorityArray = sc.broadcast(getShuffledIndex(rnd, numOfWorkset))
    val stepsize = spc.dataPerIteraiton
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
    val weight : Int = aws.getAdaWeight(rnd, worksets, process_func, evaluate_func, spc, priorityArray.value)
    this.proposedGroupNum = weight 
    
    // process data in one split, and in parallel
    worksets.foreach(workset => {
      val batchsize = math.ceil(stepsize * workset.length).toInt
      val threadActive = {
        if(thread == 1) workset.id == blockIteratorBroadcast
        else priorityArray.value(workset.id) < thread
      }
      if( threadActive ) // if the thread is active for this iteration
      {
        val rnd = new Random(process_rand_seed + workset.id)
        val sharedVar = workset.sharedVar
        sharedVar.clearDelayedDelta()
        sharedVar.batchSize = batchsize
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
        sharedVar.delayedAddShrinkFactor = 1.0
        sharedVar.delayedAddDefinedInEarlierIteration = true
        
        // construct a proposal
        workset.prop = sharedVar.exportProposal(weight)
        workset.groupID = priorityArray.value(workset.id) % weight
      }
      else{
        workset.prop = null
      }
    })
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