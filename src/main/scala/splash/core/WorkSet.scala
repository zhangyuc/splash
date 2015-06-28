package splash.core

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
  
  override def clone() = {
    val nr = new Record[U]
    nr.line = line
    nr.variable = variable
    nr.delayedDelta = delayedDelta
    nr
  }
}

class WorkSet[U: ClassTag] extends Serializable{
  var id = 0
  var sharedVar = new SharedVariableSet
  var prop : Proposal = null
  var recordArray : Array[Record[U]] = null
  var length : Long = 0
  var iterator = 0
  var cp = 0
  var seed = new Random
  
  // for auto thread detection
  var backupArray : Array[Record[U]] = null
  var backupFirstIndex = 0
  var trainGroupId = 0
  var testGroupId = 0
  
  private[splash] def backupRecord(firstIndex : Int, length : Int){
    backupArray = new Array[Record[U]](length)
    backupFirstIndex = firstIndex
    for(i <- 0 until length){
      backupArray(i) = recordArray((firstIndex+i)%recordArray.length).clone()
    }
  }
  
  private[splash] def restoreRecord() {
    for(i <- 0 until backupArray.length){
      recordArray((backupFirstIndex+i)%recordArray.length) = backupArray(i)
    }
  }
  
  private[splash] def clearBackupedRecord(){
    backupArray = null
    backupFirstIndex = 0
  }
  
  private[splash] def nextRecord() = {
    val record = recordArray(iterator)
    iterator = (iterator + 1) % recordArray.length
    record
  }
  
  private[splash] def checkpoint() = {
    cp = iterator
  }
  
  private[splash] def restore(){
    iterator = cp
  }
}