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
  var delayedDelta : Array[((String,Int), Double)] = null
}

class WorkSet[U: ClassTag] extends Serializable{
  var id = 0
  var groupID = 0
  var sharedVar = new SharedVariableSet
  var prop : Proposal = null
  var recordArray : Array[Record[U]] = null
  var length : Long = 0
  var iterator = 0
  var seed = new Random
  
  def nextRecord() = {
    val record = recordArray(iterator)
    iterator = (iterator + 1) % recordArray.length
    record
  }
  
  def getRecord( index : Int ) = { 
    recordArray(index)
  }
  
  def getRandomRecordIndex() = {
    seed.nextInt(recordArray.length)
  }
  
  def createIteratorCheckpoint() = {
    iterator
  }
  
  def restoreIteratorCheckpoint(iteratorCheckpoint: Int){
    iterator = iteratorCheckpoint
  }
  
}