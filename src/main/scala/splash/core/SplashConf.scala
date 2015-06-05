package splash.core

class SplashConf {
  var maxThreadNum = 0
  var dataPerIteration = 1.0
  var autoThread = true
  
  def set(key:String, value:String) = {
    if(key.equals("max.thread.num")){
      maxThreadNum = value.toInt
    }
    if(key.equals("data.per.iteration")){
      dataPerIteration = math.max(1.0, value.toDouble)
    }
    if(key.equals("auto.thread")){
      autoThread = value.toBoolean
    }
    this
  }
  
  def set(key:String, value:Double) : SplashConf = {
    if(key.equals("max.thread.num")){
      maxThreadNum = value.toInt
    }
    if(key.equals("data.per.iteration")){
      dataPerIteration = math.max(1.0, value)
    }
    this
  }
  
  def set(key:String, value:Boolean) : SplashConf = {
    if(key.equals("auto.thread")){
      autoThread = value
    }
    this
  }
}