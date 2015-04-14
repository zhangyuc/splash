package splash

class StreamProcessContext {
  var threadNum = 0
  var weight = 0.0
  var adaptiveWeightSampleRatio = 0.1
  var batchSize = 1.0
  var warmStart : Boolean = true
  
  def set(key:String, value:String) = {
    val spc = new StreamProcessContext
    spc.threadNum = this.threadNum
    spc.weight = this.weight
    
    if(key.equals("num.of.thread")){
      spc.threadNum = value.toInt
    }
    if(key.equals("weight")){
      spc.weight = value.toDouble
    }
    if(key.equals("adaptive.weight.sample.ratio")){
      spc.adaptiveWeightSampleRatio = value.toDouble
    }
    if(key.equals("data.per.iteration")){
      spc.batchSize = value.toDouble
    }
    if(key.equals("warm.start")){
      spc.warmStart = value.toBoolean
    }
    spc
  }
  
  def set(key:String, value:Double) = {
    val spc = new StreamProcessContext
    spc.threadNum = this.threadNum
    spc.weight = this.weight
    
    if(key.equals("num.of.thread")){
      spc.threadNum = value.toInt
    }
    if(key.equals("weight")){
      spc.weight = value.toDouble
    }
    if(key.equals("adaptive.weight.sample.ratio")){
      spc.adaptiveWeightSampleRatio = value.toDouble
    }
    if(key.equals("data.per.iteration")){
      spc.batchSize = value.toDouble
    }
    spc
  }
}