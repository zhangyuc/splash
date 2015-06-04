package splash.core
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
  
  def add(prop2:Proposal) = {
    for(pair <- prop2.delta){
      val key = pair._1
      if(delta.contains(key)){
        delta(key).delta += pair._2.delta
      }
      else{
        delta.put(key, pair._2)
      }
    }
    for(pair <- prop2.deltaArray){
      val key = pair._1
      if(deltaArray.contains(key)){
        val dv1Array = deltaArray(key).array
        val dv2Array = pair._2.array
        val n = dv1Array.length
        var i = 0
        while(i < n){
          dv1Array(i) += dv2Array(i)
          i += 1
        }
      }
      else deltaArray.put(key, pair._2)
    }
    this
  }
}