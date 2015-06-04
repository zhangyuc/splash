package splash.core
import scala.collection.mutable._

class LocalVariableSet extends Serializable{
    var variable = new HashMap[String, Double]
    
    def get(key:String) = {
      variable.applyOrElse(key, (x:Any) => 0.0)
    }
    
    def set(key:String, value:Double) {
      variable.put(key, value)
    }
    
    def this( array : Array[ (String, Double) ] ){
    this()
    if(array != null){
      for(elem <- array){
        variable.put(elem._1, elem._2)
      }
    }
  }
  
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