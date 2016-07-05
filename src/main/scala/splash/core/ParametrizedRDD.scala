package splash.core
import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import scala.reflect.ClassTag
import java.util.Date
import scalaxy.loops._
import scala.language.postfixOps

class ParametrizedRDD[T: ClassTag] extends Serializable {
  var worksets : rdd.RDD[WorkSet[T]] = _
  var numOfWorkset = 0
  var globalRnd : Random = null
  var length : Long = 0
  var totalTimeEllapsed = 0.0

  var process_func :(T, Double, SharedVariableSet, LocalVariableSet) => Any = null
  var evaluate_func : (T, SharedVariableSet, LocalVariableSet) => Double = null

  // variables for iterating worksets
  var worksetIterator = 0
  var dataProcessedSinceLastShuffling = 0.0
  var iterNum = 0

  // variables for determining thread number
  var lastIterationThreadNumber = 1
  var oldThreadNumber = 0
  var dtime = 0
  var dtimeLimit = 0

  def this(initRdd : rdd.RDD[T]){
    this()
    globalRnd = new Random
    numOfWorkset = initRdd.partitions.length
    worksets = generateWorkSets(initRdd)
    length = worksets.map( _.length ).sum().toLong
  }

  private def repartitionRDD[U: ClassTag](init_rdd : rdd.RDD[U], partition_num : Int = 0) = {
    val npar = {
      if(partition_num == 0) init_rdd.partitions.length
      else partition_num
    }
    init_rdd.map( x => (Random.nextInt(npar), x)).partitionBy(new HashPartitioner(npar)).map( x => x._2 )
  }

  /*
   * Make n-1 copies of every element without reshuffling.
   */
  def duplicate(n : Int) = {
    if(n > 0){
      duplicateWorkset(this.worksets, n)
      length *= n
    }
    this
  }

  /*
   * Make n-1 copies of every element and reshuffle them across partitions.
   * This will enlarge the dataset by a factor of n. Parallel threads can
   * reduce communication costs by passing a larger local dataset.
   */
  def duplicateAndReshuffle(n : Int) = {
    if(n > 0){
      duplicateWorksetAndReshuffle(this.worksets, n)
      length *= n
    }
    this
  }

  /*
   * Reshuffle all elements across partitions. If your original dataset has
   * not been shuffled, this operation is recommended at the creation of the
   * Parametrized RDD.
   */
  def reshuffle() = {
    duplicateWorksetAndReshuffle(this.worksets, 1)
    this
  }

  private def duplicateWorkset[U: ClassTag](worksets : rdd.RDD[WorkSet[U]], duplication : Int): Unit = {
    val dup = duplication
    val reshuffled_data = worksets.flatMap(workset => {
      workset.recordArray.iterator
    }).flatMap(x => {
      val array = new Array[Record[U]](dup)
      array(0) = x
      for(i <- 1 until dup optimized){
        array(i) = new Record[U]
        array(i).line = x.line
      }
      array.iterator
    })

    worksets.zipPartitions(reshuffled_data)( (workset_iter, iter) => {
      val workset = workset_iter.next()
      workset.recordArray = iter.toArray
      workset.length = workset.recordArray.length
      Array(0).iterator
    }).count()
  }

  private def duplicateWorksetAndReshuffle[U: ClassTag](worksets : rdd.RDD[WorkSet[U]], duplication : Int): Unit = {
    val dup = duplication
    val reshuffled_data = repartitionRDD(worksets.flatMap(workset => {
      workset.recordArray.iterator
    }).flatMap(x => {
      val array = new Array[Record[U]](dup)
      array(0) = x
      for(i <- 1 until dup optimized){
        array(i) = new Record[U]
        array(i).line = x.line
      }
      array.iterator
    }), numOfWorkset)

    worksets.zipPartitions(reshuffled_data)( (workset_iter, iter) => {
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
  def getLastIterationThreadNumber() = lastIterationThreadNumber
  def partitions = worksets.partitions

  /*
   * Set the data processing function. The function func takes an arbitrary element,
   * the weight of the element and the associated local/shared variables. It performs
   * update on the local/shared variables.
   */
  def setProcessFunction(func :(T, Double, SharedVariableSet, LocalVariableSet) => Any) = {
    process_func = func
    this
  }

  /*
   * Set a loss function for the stochastic algorithm. The function func takes an
   * element and the associated local/shared variables. It returns the loss incurred
   * by this element. Setting a loss function for the algorithm is optional, but a
   * reasonable loss function may help Splash choose a better degree of parallelism.
   */
  def setLossFunction(func: (T, SharedVariableSet, LocalVariableSet) => Double) = {
    evaluate_func = func
    this
  }

  /*
   * Return a RDD formed by mapping each element by function func. The function
   * takes the element and the associated local/shared variables as input
   */
  def map[U: ClassTag](func : (T, SharedVariableSet, LocalVariableSet) => U) = {
    val list = new ListBuffer[U]
    val f = func
    worksets.flatMap( workset => {
      for(record <- workset.recordArray){
        val localVar = new LocalVariableSet(record.variable, record.variableArray)
        workset.sharedVar.setDelayedDelta(record.delayedDelta)
        list.append(f(record.line, workset.sharedVar, localVar))
        record.variable = localVar.variableToArray()
        record.variableArray = localVar.variableArrayToArray
        record.delayedDelta = workset.sharedVar.exportDelayedDelta()
      }
      list.iterator
    })
  }

  /*
   * Reduce all elements by function func. The function takes two elements
   * as input and returns a single element as output.
   */
  def reduce(func : (T, T) => T) = {
    val f = func
    worksets.flatMap( workset => {
      val orignalCopy = workset.recordArray.map(_.line)
      orignalCopy.iterator
    }).reduce(f)
  }

  /*
   * Process each element by function func. The function takes the element
   * and the associated local/shared variables as input.
   */
  def foreach(func : (T, SharedVariableSet, LocalVariableSet) => Any) = {
    val f = func
    worksets.foreach( workset => {
      for(record <- workset.recordArray){
        val localVar = new LocalVariableSet(record.variable, record.variableArray)
        workset.sharedVar.setDelayedDelta(record.delayedDelta)
        f(record.line, workset.sharedVar, localVar)
        record.variable = localVar.variableToArray()
        record.variableArray = localVar.variableArrayToArray()
        record.delayedDelta = workset.sharedVar.exportDelayedDelta()
      }
    })
  }

  /*
   * Return a RDD formed by mapping the shared variable set by function func.
   */
  def mapSharedVariable[U: ClassTag](func : SharedVariableSet => U) = {
    val f = func
    worksets.map{
      workset => f(workset.sharedVar)
    }
  }

  /*
   * Reduce all shared variable sets by function func. The function takes two SharedVariableSet objects
   * as input and returns a single SharedVariableSet object as output.
   */
  def reduceSharedVariable(func : (SharedVariableSet, SharedVariableSet) => SharedVariableSet) = {
    val f = func
    worksets.map(_.sharedVar).reduce(f)
  }

  /*
   * Process the shared variable set by function func.
   */
  def foreachSharedVariable( preprocess_func: SharedVariableSet => Any ): Unit = {
    val pfunc = preprocess_func
    worksets.foreach( workset => {
      pfunc(workset.sharedVar)
    })
  }

  /*
   * Synchronize the shared variable across all partitions. If the shared variables is
   * changed by map/foreach but not synchronized, the change may not actually take effect.
   */
  def syncSharedVariable(): Unit = {
    val mergedProp =  worksets.map( _.sharedVar.exportProposal(1.0f) ).filter( _ != null ).treeReduce( _ add _)
    worksets.foreach( _.sharedVar.updateVariableByProposal(mergedProp) )
  }

  /*
   * Return the set of shared variables in the first partition.
   */
  def getSharedVariable() = {
    worksets.map( workset => workset.sharedVar ).first()
  }

  /*
   * Return the set of shared variables in all partitions.
   */
  def getAllSharedVariables() = {
    worksets.map( workset => workset.sharedVar ).collect()
  }

  /*
   * Use the data processing function to process the dataset. spc is a SplashConf object.
   * It specifies the hyper-parameters that the system needs.
   */
  def run(spc: SplashConf = new SplashConf()) = {
    if(iterNum == 0 || dataProcessedSinceLastShuffling >= 1.0){
      refresh()
    }
    takePass(globalRnd, worksets, process_func, evaluate_func, spc)
    iterNum += 1
    this
  }

  /*
   * remove Parametrized RDD from the memory cache.
   */
  def unpersist(): Unit = {
    worksets.unpersist()
  }

  private def refresh(): Unit = {
    val shuffle_seed = globalRnd.nextInt(65536)
    worksets.foreach( workset => {
      val rnd = new Random(shuffle_seed + workset.id)
      workset.iterator = 0

      // shuffle data points
      val nr = workset.recordArray.length
      for(j <- 0 until nr - 1 optimized){
        val randi = rnd.nextInt(nr-j-1)
        val tmp = workset.recordArray(nr-j-1)
        workset.recordArray(nr-j-1) = workset.recordArray(randi)
        workset.recordArray(randi) = tmp
      }
    })
    dataProcessedSinceLastShuffling = 0.0
  }

  private val getShuffledIndex = (n:Int) => {
    val array = new Array[Int](n)
    for(i <- 0 until n optimized){
      array(i) = i
    }
    for(j <- 0 until n - 1){
      val randi = Random.nextInt(n-j-1)
      val tmp = array(n-j-1)
      array(n-j-1) = array(randi)
      array(randi) = tmp
    }
    array
  }

  private def assignGroup(GroupAssignment : HashMap[Int, Int], num_of_workset : Int, maxThread: Int, startIndex: Int) = {
    var groupnum = 0
    var remainingThread = maxThread
    var currentThreadNumber = math.min(math.max(lastIterationThreadNumber, math.sqrt(maxThread).toInt), maxThread)
    val shuffledIndex = getShuffledIndex(maxThread)
    var counter = 0
    while(remainingThread > 0){
      if(remainingThread <= currentThreadNumber * 2) currentThreadNumber = remainingThread
      for(i <- 0 until currentThreadNumber optimized){
        GroupAssignment.put( (worksetIterator + shuffledIndex(counter)) % num_of_workset, currentThreadNumber)
        counter += 1
      }
      remainingThread -= currentThreadNumber
      currentThreadNumber = math.min(currentThreadNumber * 4, remainingThread)
      groupnum += 1
    }
    groupnum
  }

  private def takePass[U: ClassTag] ( rnd:Random, worksets: rdd.RDD[WorkSet[U]],
    process_func:(U, Double, SharedVariableSet, LocalVariableSet) => Any,
    evaluate_func: (U, SharedVariableSet, LocalVariableSet) => Double,
    spc : SplashConf ): Unit = {

    // assign variables
    val num_of_workset = numOfWorkset

    // prepare broadcast
    val func = process_func
    // val eval_func = evaluate_func
    val sc = worksets.context
    // val process_rand_seed = rnd.nextInt(65536)
    val iterStartTime = (new Date).getTime

    val maxThread = {
      if(spc.maxThreadNum == 0){
        math.min(worksets.partitions.length, worksets.context.defaultParallelism)
      }
      else{
        math.min(num_of_workset, spc.maxThreadNum)
      }
    }
    // ratio of data to be processed in each local dataset
    val stepsize = math.min(math.max(spc.dataPerIteration, num_of_workset.toDouble / length), 1.0)

    // assign threads into groups to try different thread number options
    val trainGroupAssignment = new HashMap[Int, Int]
    val testGroupAssignment = new HashMap[Int, Int]
    var activeThread = maxThread
    var groupnum = 0
    if(!spc.autoThread || evaluate_func == null){
      for(i <- 0 until maxThread optimized){ // run maxThread threads in parallel
        trainGroupAssignment.put((worksetIterator + i) % num_of_workset, maxThread)
      }
      worksetIterator = (worksetIterator + maxThread) % num_of_workset
      groupnum = 1
      activeThread = maxThread
    }
    else if(dtime < dtimeLimit){ // run lastIterationThreadNumber threads in parallel
      for(i <- 0 until lastIterationThreadNumber optimized){
        trainGroupAssignment.put((worksetIterator + i) % num_of_workset, lastIterationThreadNumber)
      }
      worksetIterator = (worksetIterator + lastIterationThreadNumber) % num_of_workset
      groupnum = 1
      dtime += 1
      activeThread = lastIterationThreadNumber
    }
    else{ // find the best thread number by cross validation
      groupnum = this.assignGroup(trainGroupAssignment, num_of_workset, maxThread, worksetIterator)
      this.assignGroup(testGroupAssignment, num_of_workset, maxThread, worksetIterator)
      worksetIterator = (worksetIterator + maxThread) % num_of_workset
      activeThread = maxThread
    }
    val groupNum = groupnum

    // process data in one split, and in parallel
    worksets.foreach(workset => {
      val batchsize = math.ceil(stepsize * workset.length).toInt
      val threadActive = trainGroupAssignment.contains(workset.id)
      if( threadActive ) // if the thread is active for this iteration
      {
        if(groupNum > 1){
          workset.sharedVar.createBackup("before.training")
          workset.backupRecord(workset.iterator, batchsize)
          workset.checkpoint()
        }
        val weight = trainGroupAssignment(workset.id)
        val sharedVar = workset.sharedVar
        sharedVar.clearDelayedDelta()
        sharedVar.batchSize = batchsize * weight
        sharedVar.delayedAddShrinkFactor = 1.0f / weight

        for(i <- 0 until batchsize optimized){
          val record = workset.nextRecord()
          val localVar = new LocalVariableSet(record.variable, record.variableArray)
          sharedVar.sampleIndex = i * weight
          sharedVar.systemExecuteDelayedAdd(record.delayedDelta)
          func(record.line, weight, sharedVar, localVar)
          record.variable = localVar.variableToArray()
          record.variableArray = localVar.variableArrayToArray()
          record.delayedDelta = sharedVar.exportDelayedDelta()
        }
        sharedVar.batchSize = 0
        sharedVar.sampleIndex = 0
        sharedVar.delayedAddShrinkFactor = 1.0f

        // construct a proposal
        workset.prop = sharedVar.exportProposal(weight)
        workset.trainGroupId = weight
      }
      else{
        workset.prop = null
        workset.trainGroupId = 0
      }
    })
    var timeElapsed = ((new Date).getTime - iterStartTime).toDouble / 1000

    if(groupNum == 1){
      // merge proposal
      val mergedPropBroadcast = {
        if(spc.treeReduce) worksets.filter( _.prop != null ).map( _.prop ).treeReduce( _ add _ )
        else worksets.filter( _.prop != null ).map( _.prop ).reduce( _ add _ )
      }
      // update by the final proposal
      worksets.foreach( workset => {
        workset.sharedVar.updateVariableByProposal(mergedPropBroadcast)
      } )
      this.lastIterationThreadNumber = activeThread
    }
    else{
      // merge proposals inside the same group
      // val shuffledIndex = getShuffledIndex(maxThread)
      val groupMergedPropArray = worksets.map( workset => (workset.trainGroupId, workset.prop) ).filter( _._2 != null ).reduceByKey( _ add _ ).collect()
      val groupMergedPropMapRaw = new HashMap[Int, Proposal]
      for(pair <- groupMergedPropArray) groupMergedPropMapRaw.put(pair._1, pair._2)
      val groupMergedPropMap = sc.broadcast(groupMergedPropMapRaw)

      // evaluate the average loss
      val evalLoss = evaluate_func
      val lossSet = worksets.flatMap(workset => {
        val threadActive = testGroupAssignment.contains(workset.id)
        if( threadActive ) // if the thread is active for this iteration
        {
          workset.testGroupId = testGroupAssignment(workset.id)
          val sharedVar = workset.sharedVar

          // if the trainGroup and the testGroup are different, then
          // restore the variable values before the training, but backup
          // the training result for future use.
          if(workset.testGroupId != workset.trainGroupId){
            sharedVar.createBackup("after.training")
            sharedVar.restoreFromBackup("before.training")
          }
          sharedVar.updateVariableByProposal(groupMergedPropMap.value(workset.testGroupId))
          var loss = 0.0
          val batchsize = math.ceil(stepsize * workset.length).toInt
          workset.restore()
          for(i <- 0 until batchsize optimized){
            val record = workset.nextRecord()
            val localVar = new LocalVariableSet(record.variable, record.variableArray)
            loss += evalLoss(record.line, sharedVar, localVar)
          }
          val res = (workset.testGroupId, (loss, batchsize))
          Array(res).iterator
        }
        else{
          workset.testGroupId = 0
          val array = new ListBuffer[(Int, (Double, Int))]
          array.iterator
        }
      }).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2) ).collect()
      val mean = lossSet.map( x => (x._1, x._2._1 / x._2._2) )

      // debug
      // mean.foreach( x => println(x) )

      // find the best thread number
      val bestThreadNum = mean.toList.sortWith( (a,b) => a._2 < b._2 ).head._1
      if(bestThreadNum == oldThreadNumber){
        dtimeLimit *= 2
      }
      else{
        dtimeLimit = 1
      }
      dtime = 1
      oldThreadNumber = bestThreadNum

      // re-update groups that are not selected
      worksets.foreach(workset => {
        if(workset.trainGroupId == bestThreadNum){
          if(workset.trainGroupId != workset.testGroupId){
            workset.sharedVar.restoreFromBackup("after.training")
            workset.sharedVar.updateVariableByProposal(groupMergedPropMap.value(bestThreadNum))
          }
        }
        else{
          workset.restoreRecord()
          workset.sharedVar.restoreFromBackup("before.training")
          workset.sharedVar.updateVariableByProposal(groupMergedPropMap.value(bestThreadNum))
        }
        workset.clearBackupedRecord()
        workset.sharedVar.clearBackup()
      })
      this.lastIterationThreadNumber = bestThreadNum
      groupMergedPropMap.destroy()
    }

    // estimate time coefficients
    if(maxThread > 1) timeElapsed = ((new Date).getTime - iterStartTime).toDouble / 1000
    this.totalTimeEllapsed += timeElapsed
    this.dataProcessedSinceLastShuffling += activeThread / num_of_workset.toDouble * stepsize
  }
}
