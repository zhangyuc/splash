package splash.core
import scala.collection.mutable._

class LocalVariableSet extends Serializable{
  var variable = new HashMap[String, Double]
    
  def this( array : Array[ (String, Double) ] ){
    this()
    if(array != null){
      for(elem <- array){
        variable.put(elem._1, elem._2)
      }
    }
  }
    
  /**
   * Return the value of associated with the key. The value is 0 if the variable has never been set.
   */
  def get(key:String) = {
    variable.applyOrElse(key, (x:Any) => 0.0)
  }
    
  /**
   * Set the variable indexed by key to be equal to value.
   */
  def set(key:String, value:Double) {
    variable.put(key, value)
  }
    
  /**
   * Convert the variable set to an array of key-value pairs.
   */
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
}