package splash.core

class SplashConf {
  var maxThreadNum = 0
  var dataPerIteration = 1.0
  var autoThread = true
  var treeReduce = true
  
  /*
   * set a property
   */
  def set(key:String, value:String) = {
    if(key.equals("max.thread.num")){
      maxThreadNum = value.toInt
    }
    if(key.equals("data.per.iteration")){
      require(value.toDouble > 0 && value.toDouble <= 1, "data.per.iteration should belong to (0,1]")
      dataPerIteration = value.toDouble
    }
    if(key.equals("auto.thread")){
      autoThread = value.toBoolean
    }
    if(key.equals("tree.reduce")){
      treeReduce = value.toBoolean
    }
    this
  }
  
  /*
   * set a property
   */
  def set(key:String, value:Double) : SplashConf = {
    if(key.equals("max.thread.num")){
      maxThreadNum = value.toInt
    }
    if(key.equals("data.per.iteration")){
      require(value.toDouble > 0 && value.toDouble <= 1, "data.per.iteration should belong to (0,1]")
      dataPerIteration = value.toDouble
    }
    this
  }
  
  /*
   * set a property
   */
  def set(key:String, value:Boolean) : SplashConf = {
    if(key.equals("auto.thread")){
      autoThread = value
    }
    if(key.equals("tree.reduce")){
      treeReduce = value
    }
    this
  }
}