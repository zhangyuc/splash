package splash

import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.collection.mutable._
import scala.util.Random
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

class Record[U: ClassTag] extends Serializable{
  var line : U = _
  var variable : Array[(String, Double)] = null
}

class Block[U: ClassTag] extends Serializable{
  var id = 0
  var recordArray : Array[Record[U]] = null
}

class WorkSet[U: ClassTag] extends Serializable{
  var id = 0
  var sharedVar = new ParameterSet
  var prop : Proposal = null
  var blockArray : Array[Block[U]] = null
  var length : Long = 0
  var iterator = (0,0)
  var seed = new Random
  
  def nextRecord() = {
    val record = blockArray(iterator._1).recordArray(iterator._2)
    if(iterator._2 < blockArray(iterator._1).recordArray.length - 1){
      iterator = (iterator._1, iterator._2 +1 )
    }
    else{
      iterator = ((iterator._1+1) % blockArray.length, 0)
      while(blockArray(iterator._1).recordArray.length == 0){
        iterator = ((iterator._1+1) % blockArray.length, 0)
      }
    }
    record
  }
  
  def getRecord( index : (Int,Int) ) = { 
    blockArray(index._1).recordArray(index._2)
  }
  
  def getRandomRecordIndex() = {
    val block = blockArray(seed.nextInt(blockArray.length))
    (block.id, seed.nextInt(block.recordArray.length))
  }
  
  def createIteratorCheckpoint() = {
    iterator
  }
  
  def restoreIteratorCheckpoint(iteratorCheckpoint: (Int,Int)){
    iterator = iteratorCheckpoint
  }
  
}