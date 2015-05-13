package splash

class StreamProcessContext {
  var threadNum = 0
  var groupNum = 0
  var adaptiveWeightSampleRatio = 0.1
  var warmStart : Boolean = true
  var dataPerIteraiton = 1.0
  
  def set(key:String, value:String) = {
    val spc = new StreamProcessContext
    spc.threadNum = this.threadNum
    spc.groupNum = this.groupNum
    spc.adaptiveWeightSampleRatio = this.adaptiveWeightSampleRatio
    spc.warmStart = this.warmStart
    spc.dataPerIteraiton = this.dataPerIteraiton
    
    if(key.equals("num.of.thread")){
      spc.threadNum = value.toInt
    }
    if(key.equals("num.of.group")){
      spc.groupNum = value.toInt
    }
    if(key.equals("data.per.iteration")){
      spc.dataPerIteraiton = value.toDouble
    }
    if(key.equals("warm.start")){
      spc.warmStart = value.toBoolean
    }
    spc
  }
  
  def set(key:String, value:Double) = {
    val spc = new StreamProcessContext
    spc.threadNum = this.threadNum
    spc.groupNum = this.groupNum
    spc.adaptiveWeightSampleRatio = this.adaptiveWeightSampleRatio
    spc.warmStart = this.warmStart
    spc.dataPerIteraiton = this.dataPerIteraiton
    
    if(key.equals("num.of.thread")){
      spc.threadNum = value.toInt
    }
    if(key.equals("num.of.group")){
      spc.groupNum = value.toInt
    }
    if(key.equals("data.per.iteration")){
      spc.dataPerIteraiton = value.toDouble
    }
    spc
  }
}