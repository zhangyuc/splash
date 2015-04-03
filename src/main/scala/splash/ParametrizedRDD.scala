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
  var numOfBlock = 0
  var numOfWorkset = 0
  var globalRnd : Random = null
  var streamPartitioner : Partitioner = null
  
  var length : Long = 0
  var totalTimeEllapsed = 0.0
  var proposedWeight = 0.0
  
  var process_func :(Random, T, Double, ParameterSet, ParameterSet) => Any = null
  var evaluate_func : (T, ParameterSet, ParameterSet) => Double = null
  var postprocess_func : (ParameterSet) => Any = null
  var worksetIterator = 0
  
  var iterNum = 0
  
  def this( initRdd : rdd.RDD[T], rand : Random = new Random, partitioner : Partitioner = new HashPartitioner(1) ){
    this()
    length = initRdd.count()
    streamPartitioner = partitioner
    numOfBlock = streamPartitioner.numPartitions
    numOfWorkset = initRdd.partitions.length
    globalRnd = rand
    worksets = generateWorkSets(initRdd)
  }
  
  def setProcessFunction(func :(Random, T, Double, ParameterSet, ParameterSet) => Any){
    process_func = func
  }
  
  def setLossFunction(func: (T, ParameterSet, ParameterSet) => Double){
    evaluate_func = func
  }
  
  def setPostprocessFunction(func: (ParameterSet) => Any){
    postprocess_func = func
  }
  
  def getTotalTimeEllapsed = totalTimeEllapsed
  def getSelectedSampleWeight = proposedWeight
  
  def map[U: ClassTag](func : (T, ParameterSet, ParameterSet) => U) = {
    val list = new ListBuffer[U]
    val f = func
    worksets.flatMap( workset => {
      workset.sharedVar.batchSize = workset.length
      workset.sharedVar.rescaleFactor = 1.0
      for(block <- workset.blockArray){
        for(record <- block.recordArray){
          val localVar = new ParameterSet(record.variable)
          list.append(f(record.line, workset.sharedVar, localVar))
          record.variable = localVar.toArray()
        }
      }
      list.iterator
    })
  }
  
  def foreach(func : (T, ParameterSet, ParameterSet) => Any) = {
    val f = func
    worksets.foreach( workset => {
      workset.sharedVar.batchSize = workset.length
      workset.sharedVar.rescaleFactor = 1.0
      for(block <- workset.blockArray){
        for(record <- block.recordArray){
          val localVar = new ParameterSet(record.variable)
          f(record.line, workset.sharedVar, localVar)
          record.variable = localVar.toArray()
        }
      }
    })
  }
  
  def syncSharedVariable(){
    worksets.foreach( workset => {
        workset.prop = workset.sharedVar.exportProposal()
    })
    val mergedProp = worksets.context.broadcast(worksets.map( workset => workset.prop ).reduce( (prop1:Proposal, prop2: Proposal) => {
      if( prop1 == null && prop2 == null){
        null
      }
      else if (prop1 == null){
        prop2
      }
      else if(prop2 == null){
        prop1
      }
      else{
        prop1 merge prop2
      }
    }))
    worksets.foreach( workset => {
      workset.sharedVar.updateByProposal(mergedProp.value)
    } )
    mergedProp.destroy()
  }
  
  def mapSharedVariable[U: ClassTag](func : ParameterSet => U) = {
    val f = func
    worksets.map{
      workset => f(workset.sharedVar)
    }
  }
  
  def foreachSharedVariable( preprocess_func: (ParameterSet) => Any ) {
    val pfunc = preprocess_func
    val preprocess_rand_seed = globalRnd.nextInt(65536)
    worksets.foreach( workset => {
      pfunc(workset.sharedVar)
    })
  }
  
  def streamProcess (spc: StreamProcessContext){
    refresh()
    if(iterNum == 0 && spc.warmStart && spc.numOfThread > 1 && spc.applyAdaptiveReweighting){
      takePass(globalRnd, worksets, process_func, evaluate_func, postprocess_func, spc.set("num.of.thread", "1"))
      takePass(globalRnd, worksets, process_func, evaluate_func, postprocess_func, spc)
    }
    else{
      takePass(globalRnd, worksets, process_func, evaluate_func, postprocess_func, spc)
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
    val processed_data = worksets.flatMap[Record[T]]( a => {
      val recordList = new ListBuffer[Record[T]]
      for( block <- a.blockArray ){
        recordList ++= block.recordArray
      }
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
      for(block <- workset.blockArray){
        for(record <- block.recordArray){
          data_list.append(record)
        }
      }
      data_list.iterator
    }).repartition(numOfWorkset)
    
    val tmp = worksets.zipPartitions(reshuffled_data)((workset_iter, iter) => {
      val workset = workset_iter.next()
      workset.blockArray = new Array[Block[U]](1)
      workset.blockArray(0) = new Block[U]
      workset.blockArray(0).recordArray = iter.toArray
      workset.length = workset.blockArray(0).recordArray.length
      Array(0).iterator
    }).count()
  }
  
  def refresh() {
    val shuffle_seed = globalRnd.nextInt(65536)
    worksets.foreach( workset => {
      val rnd = new Random(shuffle_seed + workset.id)
      val nb = workset.blockArray.length
      workset.iterator = (0,0)
      
      // shuffle blocks
      for(i <- 0 until nb - 1){
        val randi = rnd.nextInt(nb-i-1)
        val tmp = workset.blockArray(nb-i-1)
        workset.blockArray(nb-i-1) = workset.blockArray(randi)
        workset.blockArray(randi) = tmp
      }
      
      // shuffle data points
      for(block <- workset.blockArray){
        val nr = block.recordArray.length
        for(j <- 0 until nr - 1){
          val randi = rnd.nextInt(nr-j-1)
          val tmp = block.recordArray(nr-j-1)
          block.recordArray(nr-j-1) = block.recordArray(randi)
          block.recordArray(randi) = tmp
        }
      }
    })
  }
  
  private def generateWorkSets[U: ClassTag] ( single_row_rdd:rdd.RDD[U] ) = {
    val num_of_block = numOfBlock
    val partitioner = streamPartitioner
    
    val result = single_row_rdd.mapPartitionsWithIndex( (index, iter) => {
      val workset = new WorkSet[U]
      workset.id = index
      workset.blockArray = new Array[Block[U]](num_of_block)
      val tmp_list = new Array[ListBuffer[Record[U]]](num_of_block)
      for(i <- 0 until num_of_block){
        workset.blockArray(i) = new Block[U]
        tmp_list(i) = new ListBuffer[Record[U]]
      }
      
      while(iter.hasNext){
        val line = iter.next()
        tmp_list(partitioner.getPartition(line)).append( {
          val r = new Record[U]
          r.line = line
          r })
        workset.length += 1
      }
      
      for(i <- 0 until num_of_block){
        workset.blockArray(i).id = i
        workset.blockArray(i).recordArray = tmp_list(i).toArray
      }
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
    process_func:(Random, U, Double, ParameterSet, ParameterSet) => Any,
    evaluate_func: (U, ParameterSet, ParameterSet) => Double, 
    postprocess_func : (ParameterSet) => Any,
    spc : StreamProcessContext ) {
    
    // assign variables
    val applyAdaptiveReweighting = {
      spc.applyAdaptiveReweighting == true && spc.numOfThread > 1
    }
    
    val beta = 0.9
    val num_of_workset = numOfWorkset
    var stepsizeSum = 0.0
    var iterNum = 0
    var totalTimeSum = 0.0
      
    while(stepsizeSum < 1)
    {
      // prepare broadcast
      val blockIteratorBroadcast = worksetIterator
      if(spc.numOfThread == 1){ 
        worksetIterator = (worksetIterator + 1) % num_of_workset 
      }
      
      val func = process_func
      val eval_func = evaluate_func
      val num_of_thread = spc.numOfThread
      val post_func = postprocess_func
      
      val sc = worksets.context
      val priorityArray = sc.broadcast(getShuffledIndex(rnd, numOfWorkset))
      
      val stepsize = spc.batchSize
      val process_rand_seed = rnd.nextInt(65536)
      val adaptiveReweightingSampleRatio = spc.adaptiveReweightingSampleRatio
      val iterStartTime = (new Date).getTime
      
      // determine reweight
      val reweight = {
        if(applyAdaptiveReweighting == true){
          aws.getAdaWeight(rnd, worksets, process_func, evaluate_func, postprocess_func, spc, priorityArray.value)
        }
        else{
          spc.reweight
        }
      }
      this.proposedWeight = reweight
      
      // process data in one split, and in parallel
      worksets.foreach(workset => {
        val batchsize = math.ceil(stepsize * workset.length).toInt
        val threadActive = {
          if(num_of_thread == 1) workset.id == blockIteratorBroadcast
          else priorityArray.value(workset.id) < num_of_thread
        }
        if( threadActive ) // if the thread is active for this iteration
        {
          val rnd = new Random(process_rand_seed + workset.id)
          val sharedVar = workset.sharedVar
          sharedVar.batchSize = batchsize
          sharedVar.rescaleFactor = reweight
          
          for(i <- 0 until batchsize){
            val record = workset.nextRecord()
            val localVar = new ParameterSet(record.variable)
            func(rnd, record.line, sharedVar.rescaleFactor, sharedVar, localVar)
            record.variable = localVar.toArray()
          }
          
          // construct a proposal
          val endTime = (new Date).getTime
          workset.prop = sharedVar.exportProposal()
        }
        else{
          workset.prop = null
        }
      })
      
      // merge proposals
      val mergedProp = sc.broadcast(worksets.map( workset => workset.prop ).reduce( (prop1:Proposal, prop2: Proposal) => {
        if( prop1 == null && prop2 == null){
          null
        }
        else if (prop1 == null){
          prop2
        }
        else if(prop2 == null){
          prop1
        }
        else{
          prop1 merge prop2
        }
      }))
      
      // postprocessing
      val postprocess_rand_seed = rnd.nextInt(65536)
      worksets.foreach( workset => {
        workset.sharedVar.updateByProposal(mergedProp.value)
        workset.prop = null
        if(post_func != null){
          post_func(workset.sharedVar)
          workset.sharedVar.applyDelta()
        }
      } )
      mergedProp.destroy()
      
      // estimate time coefficients
      val timeElapsed = ((new Date).getTime - iterStartTime).toDouble / 1000
      this.totalTimeEllapsed += timeElapsed
      stepsizeSum += stepsize
      totalTimeSum += timeElapsed
      iterNum += 1
    }
  }
}