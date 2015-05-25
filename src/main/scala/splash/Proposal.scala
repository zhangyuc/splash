package splash

import scala.collection.mutable._

class ProposalDeltaValue extends Serializable{
  var delta = 0.0
  var prefactor = 1.0
}

class ProposalDeltaValueArray extends Serializable{
  var prefactor = 1.0
  var array : Array[Double] = null
}

class Proposal extends Serializable{
  var delta = new HashMap[String, ProposalDeltaValue]
  var deltaArray = new HashMap[String, ProposalDeltaValueArray]
  
  def dispose() {
    delta.clear()
    deltaArray.clear()
  }
  
  def concatinate(prop2:Proposal) = {
    val new_prop = new Proposal
    new_prop.delta = this.delta
    for(pair <- prop2.delta){
      val key = pair._1
      val dv2 = pair._2
      if(new_prop.delta.contains(key)){
        val dv = new_prop.delta(key)
        dv.delta = dv2.prefactor * dv.delta + dv2.delta
        dv.prefactor *= dv2.prefactor
      }
      else{
        new_prop.delta.put(key, dv2)
      }
    }
    
    new_prop.deltaArray = this.deltaArray
    for(pair <- prop2.deltaArray){
      val key = pair._1
      val dv2 = pair._2.array
      val dv2_prefactor = pair._2.prefactor
      
      if(new_prop.deltaArray.contains(key)){ // concatenate prop2 after prop1
        val dv = new_prop.deltaArray(key).array
        new_prop.deltaArray(key).prefactor *= dv2_prefactor // concatenate prefactor
        for(i <- 0 until dv.array.length){
          dv(i) = dv2_prefactor * dv(i) + dv2(i) // concatenate delta
        }
      }
      else{
        new_prop.deltaArray.put(key, pair._2)
      }
    }
    
    new_prop
  }
  
  def add(prop2:Proposal) = {
    val new_prop = new Proposal
    new_prop.delta = this.delta
    for(pair <- prop2.delta){
      val key = pair._1
      if(new_prop.delta.contains(key)){
        new_prop.delta(key).delta += pair._2.delta
      }
      else{
        new_prop.delta.put(key, pair._2)
      }
    }
    
    new_prop.deltaArray = this.deltaArray
    for(pair <- prop2.deltaArray){
      val key = pair._1
      if(new_prop.deltaArray.contains(key)){
        val dvObject = new_prop.deltaArray(key)
        for(i <- 0 until dvObject.array.length){
          dvObject.array(i) += pair._2.array(i)
        }
      }
      else{
        new_prop.deltaArray.put(key, pair._2)
      }
    }
    
    new_prop
  }
}