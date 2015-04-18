package splash

import scala.collection.mutable._

object UpdateType extends Enumeration with Serializable{
  val Keep, Push = Value
}

class ParameterSet extends Serializable{
  var variable = new HashMap[String, Double]
  var delta = new HashMap[String, (Double, Double, UpdateType.Value)]
  var rescaleFactor = 1.0
  var batchSize : Long = 0
   
  def this( array : Array[ (String, Double) ] ){
    this()
    if(array != null){
      for(elem <- array){
        variable.put(elem._1, elem._2)
      }
    }
  }
  
  def getBatchSize() = batchSize * rescaleFactor
  
  def toArray () = {
    val n = variable.count( a => true )
    if( n > 0)
    {
      val array = new Array[ (String, Double) ](n)
      var index = 0
      for(elem <- variable){
        array(index) = elem
        index += 1
      }
      array
    }
    else{
      null
    }
  }
  
  def getOldValue(key:String) = {
    variable.applyOrElse(key, (x:Any) => 0.0)
  }
  
  def get(key:String) = {
      val dv = delta.applyOrElse(key, (x:Any) => (0.0,0.0,null) )
      variable.applyOrElse(key, (x:Any) => 0.0) + dv._1 + dv._2
  }
  
  def set(key:String, value:Double) = {
    variable.put(key, value)
  }
  
  def update(key:String, value:Double, updateType: UpdateType.Value = UpdateType.Push) = {
    val dv = delta.applyOrElse(key, (x:Any) => (0.0,0.0,null) )
    delta.put(key, (dv._1 + value, dv._2, updateType))
  }
  
  def updateWithUnitWeight(key:String, value:Double, updateType: UpdateType.Value = UpdateType.Push) = {
    val dv = delta.applyOrElse(key, (x:Any) => (0.0,0.0,null) )
    delta.put(key, (dv._1, dv._2 + value, updateType))
  }
  
  def updateByProposal(prop:Proposal) = {
    for( key <- prop.delta.keySet ){
      variable.put(key, getOldValue(key) + prop.delta(key) )
    }
    this
  }
    
  def applyDelta(target_delta: HashMap[String, Double], coefficient : Double = 1.0){
    for(pair <- target_delta){
      this.set(pair._1, getOldValue(pair._1) + coefficient * pair._2 )
    }
    delta.clear()
  }
  
  def applySelf(backup : Boolean = false) = {
    val target_delta = delta
    val backup_delta = new HashMap[String, Double]
    for(pair <- target_delta){
      this.set(pair._1, getOldValue(pair._1) + pair._2._1 / rescaleFactor + pair._2._2 )
      if(backup){
        backup_delta.put(pair._1, pair._2._1 / rescaleFactor + pair._2._2)
      }
    }
    delta.clear()
    backup_delta
  }
  
  def clear() {
    delta.clear()
  }
  
  def exportProposal() = {
    val prop = new Proposal
    
    for(pair <- delta){
      if( pair._2._3 == UpdateType.Push){
        prop.delta.put(pair._1, pair._2._1 / rescaleFactor + pair._2._2)
      }
      else{
        this.set(pair._1, getOldValue(pair._1) + pair._2._1 / rescaleFactor + pair._2._2 )
      }
    }
    delta.clear()
    prop
  }
}