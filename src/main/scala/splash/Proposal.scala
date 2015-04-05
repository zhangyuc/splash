package splash

import scala.collection.mutable._

class Proposal extends Serializable{
  var delta = new HashMap[String, Double]
  var loss = new HashSet[(Double, Double)]
  
  def dispose() {
    delta.clear()
  }
  
  def merge(prop2:Proposal) = {
    val new_prop = new Proposal
    
    // merge the ending state of the two proposals
    new_prop.delta = this.delta
    for(key <- prop2.delta.keySet){
      new_prop.delta.put(key, new_prop.delta.applyOrElse(key, (x:Any)=>0.0) + prop2.delta(key) )
    }
    
    // return the new proposal
    new_prop
  }
}