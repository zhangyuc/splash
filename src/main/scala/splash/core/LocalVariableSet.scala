package splash.core
import scala.collection.mutable._

class LocalVariableSet extends Serializable{
  var variable = new HashMap[String, Float]
  var variableArray = new HashMap[String, Array[Float]]

  def this( v : Array[(String, Float)], va : Array[(String, Array[Float])] ){
    this()
    if(v != null){
      for(elem <- v){
        variable.put(elem._1, elem._2)
      }
    }
    if(va != null){
      for(elem <- va){
        variableArray.put(elem._1, elem._2)
      }
    }
  }

  /**
   * Return the value of associated with the key. The value is 0 if the variable has never been set.
   */
  def get(key:String) = {
    variable.applyOrElse(key, (_:Any) => 0.0f).toDouble
  }

  /**
   * Set the variable indexed by key to be equal to value.
   */
  def set(key:String, value:Double): Unit = {
    variable.put(key, value.toFloat)
  }

  /**
   * Return an array of variables given the key.
   * The array must have been set before.
   */
  def getArray(key:String) = {
    variableArray(key)
  }

  /**
   * Set an array given the key.
   */
  def setArray(key:String, value:Array[Float]): Unit = {
    variableArray.put(key, value)
  }

  /**
   * Convert variable to an array of key-value pairs.
   */
  def variableToArray () = {
    val n = variable.count( _ => true )
    if( n > 0)
    {
      val array = new Array[ (String, Float) ](n)
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

  /**
   * Convert variableArray to an array of key-value pairs.
   */
  def variableArrayToArray () = {
    val n = variableArray.count( _ => true )
    if( n > 0)
    {
      val array = new Array[ (String, Array[Float])](n)
      var index = 0
      for(elem <- variableArray){
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
