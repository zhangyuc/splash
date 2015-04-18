package splash

import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import scala.reflect.ClassTag

class AdaWeightScheduler {
  var oldWeight = 0.0
  var dtime = 0
  var dtimeLimit = 0
  
  def getAdaWeight[U: ClassTag] ( rnd:Random, worksets: rdd.RDD[WorkSet[U]],
    process_func:(Random, U, Double, ParameterSet, ParameterSet) => Any,
    evaluate_func: (U, ParameterSet, ParameterSet) => Double = null, 
    postprocess_func : (ParameterSet) => Any,
    spc : StreamProcessContext,
    parray : Array[Int]) = {
    
    if(spc.threadNum == 1 || spc.weight != 0){
      math.max(1, spc.weight)
    }
    else if(dtime < dtimeLimit){
      dtime += 1
      oldWeight
    }
    else{
      val func = process_func
      val eval_func = evaluate_func
      val post_func = postprocess_func
      val threadNum = spc.threadNum
      val foldNum = spc.adaptiveWeightFoldNum
      val adaptiveReweightingSampleRatio = spc.adaptiveWeightSampleRatio * spc.batchSize
      val process_rand_seed = rnd.nextInt(65536)
      val priorityArray = worksets.context.broadcast(parray)
      
      // construct rescaling factor set
      val rescaleFactorSet = new ListBuffer[Double]
      var factor = 1
      while(factor < threadNum){
        rescaleFactorSet.append(factor)
        factor *= 4
      }
      rescaleFactorSet.append(threadNum)
      
      // construct loss set
      val lossSet = worksets.map(workset => {
        val tentativeBatchSize = math.ceil(workset.length * adaptiveReweightingSampleRatio).toInt
        val sharedVar = workset.sharedVar
        val lossSet = new HashSet[(Double,Double)]
        val factor = rescaleFactorSet( priorityArray.value(workset.id) % rescaleFactorSet.length )
        
        if(priorityArray.value(workset.id) < threadNum){
          val rnd = new Random(process_rand_seed + workset.id)
          sharedVar.batchSize = tentativeBatchSize
          
          // construct test set
          val testDataIndex = new Array[Array[(Int,Int)]](foldNum)
          for(testid <- 0 until foldNum)
          {
            testDataIndex(testid) = new Array[(Int,Int)](tentativeBatchSize)
            for(i <- 0 until tentativeBatchSize){
              testDataIndex(testid)(i) = (workset.createIteratorCheckpoint())
              workset.nextRecord()
            }
          }
          
          // define evaluation function
          val evaluate = () => {
            var loss = 0.0
            for(testid <- 0 until foldNum)
            {
              for(index <- testDataIndex(testid)){
                val record = workset.getRecord(index)
                if(eval_func != null){
                  loss += eval_func(record.line, sharedVar, new ParameterSet(record.variable))
                }
                else{
                  sharedVar.delta.clear()
                  func(rnd, record.line, 1.0, sharedVar, new ParameterSet(record.variable))
                  var localLoss = 0.0
                  for(pair <- sharedVar.delta){
                    localLoss += math.pow(pair._2._1 + pair._2._2, 2)
                  }
                  loss += localLoss
                }
              }
            }
            loss
          }
          
          // loss before training
          var loss = 0.0
          
          // backup local variable
          val localVarBackup = new ListBuffer[((Int,Int),Array[(String, Double)])]
          for(testid <- 0 until foldNum)
          {
            for(index <- testDataIndex(testid)){
              localVarBackup.append((index, workset.getRecord(index).variable))
            }
          }
          
          // training
          val delta_all = new HashMap[String, Double]
          for(testid <- 0 until foldNum){
            sharedVar.delta.clear()
            for(index <- testDataIndex(testid)){ 
              val record = workset.getRecord(index)
              // process single data point
              val localVar = new ParameterSet(record.variable)
              func(rnd, record.line, factor * workset.length / tentativeBatchSize, sharedVar, localVar)
              record.variable = localVar.toArray()
            }
            // aggregate updates
            for(pair <- sharedVar.delta){
              val old_value = delta_all.applyOrElse(pair._1, (x:Any)=>0.0)
              delta_all.put(pair._1, old_value + pair._2._1 * threadNum / factor / foldNum + pair._2._2)
            }
          }
          for(pair <- delta_all){
            sharedVar.delta.put(pair._1, (0, pair._2, null))
          }
          
          // postprocessing
          if(post_func != null){
            post_func(sharedVar)
          }
          val deltaBackup = sharedVar.applySelf(true)
          
          // loss after training
          loss += evaluate() / tentativeBatchSize / foldNum
          
          // restore shared variable
          sharedVar.applyDelta(deltaBackup, -1)
          
          // restore local variable
          for(entry <- localVarBackup){ 
            val record = workset.getRecord(entry._1)
            record.variable = entry._2
          }
          
          // restore checkpoint
          lossSet.add(factor, loss)
        }
        lossSet
      }).reduce( (s1,s2) => s1 ++ s2 )
            
      // compute mean and standard deviation
      var count = new HashMap[Double,Double]
      var mean = new HashMap[Double,Double]
      var std = new HashMap[Double,Double]
      for(loss <- lossSet){
        count.put(loss._1, count.applyOrElse(loss._1, (x:Any)=>0.0) + 1) 
        mean.put(loss._1, mean.applyOrElse(loss._1, (x:Any)=>0.0) + loss._2)
        std.put(loss._1, std.applyOrElse(loss._1, (x:Any)=>0.0) + loss._2 * loss._2)
      } 
      mean = mean.map( x => (x._1, x._2 / count(x._1)))
      std = std.map( x => (x._1, math.sqrt( x._2 / count(x._1) / count(x._1) - math.pow(mean(x._1), 2) / count(x._1) ) ) )
      
      // output
       println()
       mean.foreach( x => println(x._1 +  "\t" +  x._2 + "\t" + std(x._1)) )
      
      val sortedSum = mean.toList.sortWith( (a,b) => a._2 < b._2 )
      val newWeight = sortedSum(0)._1
      
      if(newWeight == oldWeight){
        dtimeLimit *= 2
      }
      else{
        dtimeLimit = 1
      }
      dtime = 1
      oldWeight = newWeight
      newWeight
    }
  }
}