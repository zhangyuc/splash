package splash

import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import scala.reflect.ClassTag

class AdaWeightScheduler {
  var oldWeight = 0
  var dtime = 0
  var dtimeLimit = 0
  
  def getAdaWeight[U: ClassTag] ( rnd:Random, worksets: rdd.RDD[WorkSet[U]],
    process_func:(U, Double, SharedVariableSet, LocalVariableSet) => Any,
    evaluate_func: (U, SharedVariableSet, LocalVariableSet) => Double = null, 
    spc : StreamProcessContext,
    parray : Array[Int]) = {
    
    if(spc.threadNum == 1 || spc.groupNum != 0){
      math.max(1, spc.groupNum)
    }
    else if(dtime < dtimeLimit){
      dtime += 1
      oldWeight
    }
    else{
      val func = process_func
      val eval_func = evaluate_func
      val threadNum = {
        if(spc.threadNum == 0){
          worksets.partitions.length
        }
        else{
          spc.threadNum
        }
      }
      val adaptiveReweightingSampleRatio = math.min(spc.adaptiveWeightSampleRatio, 1.0 / spc.dataPerIteraiton)
      val dataPerIteration = spc.dataPerIteraiton
      val process_rand_seed = rnd.nextInt(65536)
      val priorityArray = worksets.context.broadcast(parray)
     
      
      // construct rescaling factor set
      val weightSet = new ListBuffer[Int]
      var weight = 1
      while(weight < threadNum){
        weightSet.append(weight)
        weight *= 4
      }
      weightSet.append(threadNum)
      
      // construct loss set
      val lossSet = worksets.map(workset => {
        val length = workset.length * dataPerIteration
        val tentativeBatchSize = math.ceil(length * adaptiveReweightingSampleRatio).toInt
        val sharedVar = workset.sharedVar
        val lossSet = new HashSet[(Int,Double)]
        val weight = weightSet( priorityArray.value(workset.id) % weightSet.length )
        
        if(priorityArray.value(workset.id) < threadNum){
          val rnd = new Random(process_rand_seed + workset.id)
          sharedVar.batchSize = tentativeBatchSize
          
          // construct test set
          val testDataIndex = new Array[Int](tentativeBatchSize)
          for(i <- 0 until tentativeBatchSize){
            testDataIndex(i) = workset.createIteratorCheckpoint()
            workset.nextRecord()
          }
          
          // define evaluation function
          val evaluate = () => {
            var loss = 0.0
            for(index <- testDataIndex){
              val record = workset.getRecord(index)
              if(eval_func != null){
                loss += eval_func(record.line, sharedVar, new LocalVariableSet(record.variable))
              }
              else{
                sharedVar.delta.clear()
                sharedVar.deltaArray.clear()
                func(record.line, 1.0, sharedVar, new LocalVariableSet(record.variable))
                var localLoss = 0.0
                for(pair <- sharedVar.delta){
                  localLoss += math.pow(pair._2.delta + pair._2.unweightedDelta, 2)
                }
                sharedVar.refreshAllDeltaArrayElementPrefactor()
                for(pair <- sharedVar.deltaArray){
                  for(dv <- pair._2.array){
                    localLoss += math.pow(dv.delta + dv.unweightedDelta, 2)
                  }
                }
                loss += localLoss
              }
            }
            loss
          }
          
          // loss before training
          var loss = 0.0
          
          // backup local variables
          val localVarBackup = new ListBuffer[(Int,Array[(String, Double)])]
          for(index <- testDataIndex){
            localVarBackup.append((index, workset.getRecord(index).variable))
          }
          
          // training
          sharedVar.delta.clear()
          sharedVar.deltaArray.clear()
          
          for(index <- testDataIndex){ 
            val record = workset.getRecord(index)
            // process single data point
            val localVar = new LocalVariableSet(record.variable)
            sharedVar.executeDelayedAdd(record.delayedDelta)
            func(record.line, weight * length / tentativeBatchSize, sharedVar, localVar)
            record.variable = localVar.toArray()
            sharedVar.clearDelayedDelta()
          }
          
          // aggregate updates for delta
          for(pair <- sharedVar.delta){
            val duplidate = threadNum / weight 
            val prefactor = math.pow(pair._2.prefactor, duplidate)
            val postfactor = {
              if(pair._2.prefactor == 1){
                duplidate
              }
              else{
                (1 - math.pow(pair._2.prefactor, duplidate)) / (1 - pair._2.prefactor)
              }
            }
            pair._2.prefactor = prefactor
            pair._2.delta = postfactor * pair._2.delta
          }
          
          // aggregate updates for delta array
          sharedVar.refreshAllDeltaArrayElementPrefactor()
          for(pair <- sharedVar.deltaArray){
            val key = pair._1
            val duplidate = threadNum / weight 
            val prefactor = math.pow(pair._2.prefactor, duplidate)
            val postfactor = {
              if(pair._2.prefactor == 1){
                duplidate
              }
              else{
                (1 - math.pow(pair._2.prefactor, duplidate)) / (1 - pair._2.prefactor)
              }
            }
            pair._2.prefactor = prefactor
            for(dv <- pair._2.array)
            {
              dv.prefactor = prefactor
              dv.delta = postfactor * dv.delta
            }
          }
          
          // loss after training
          loss += evaluate() / tentativeBatchSize
          
          // restore shared variable
          sharedVar.delta.clear()
          sharedVar.deltaArray.clear()
          
          // restore local variable
          for(entry <- localVarBackup){ 
            val record = workset.getRecord(entry._1)
            record.variable = entry._2
          }
          
          // restore checkpoint
          lossSet.add(weight, loss)
        }
        lossSet
      }).reduce( (s1,s2) => s1 ++ s2 )
      
      // compute mean and standard deviation
      var count = new HashMap[Int,Double]
      var mean = new HashMap[Int,Double]
      var std = new HashMap[Int,Double]
      for(loss <- lossSet){
        count.put(loss._1, count.applyOrElse(loss._1, (x:Any)=>0.0) + 1) 
        mean.put(loss._1, mean.applyOrElse(loss._1, (x:Any)=>0.0) + loss._2)
        std.put(loss._1, std.applyOrElse(loss._1, (x:Any)=>0.0) + loss._2 * loss._2)
      } 
      mean = mean.map( x => (x._1, x._2 / count(x._1)))
      std = std.map( x => (x._1, math.sqrt( x._2 / count(x._1) / count(x._1) - math.pow(mean(x._1), 2) / count(x._1) ) ) )
      
      // output
      // println()
      // mean.foreach( x => println(x._1 +  "\t" +  x._2 + "\t" + std(x._1)) )
      
      val sortedSum = mean.toList.sortWith( (a,b) => a._2 < b._2 )
      var newWeight = sortedSum(0)._1
      
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