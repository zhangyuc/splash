package splash

class StreamProcessContext {
  var numOfThread = 1
  var reweight = 1.0
  var applyAdaptiveReweighting : Boolean = true
  var warmStart : Boolean = true
  var adaptiveReweightingSampleRatio = 0.1
  var batchSize = 1.0
  
  def set(key:String, value:String) = {
    val spc = new StreamProcessContext
    spc.numOfThread = this.numOfThread
    spc.reweight = this.reweight
    spc.applyAdaptiveReweighting = this.applyAdaptiveReweighting
    
    if(key.equals("num.of.thread")){
      spc.numOfThread = value.toInt
    }
    if(key.equals("reweight")){
      spc.reweight = value.toDouble
    }
    if(key.equals("apply.adaptive.reweighting")){
      spc.applyAdaptiveReweighting = value.toBoolean
    }
    if(key.equals("adaptive.reweighting.sample.ratio")){
      spc.adaptiveReweightingSampleRatio = value.toDouble
    }
    if(key.equals("data.per.iteration")){
      spc.batchSize = value.toDouble
    }
    if(key.equals("apply.warm.start")){
      spc.warmStart = value.toBoolean
    }
    spc
  }
  
  def set(key:String, value:Double) = {
    val spc = new StreamProcessContext
    spc.numOfThread = this.numOfThread
    spc.reweight = this.reweight
    spc.applyAdaptiveReweighting = this.applyAdaptiveReweighting
    
    if(key.equals("num.of.thread")){
      spc.numOfThread = value.toInt
    }
    if(key.equals("reweight")){
      spc.reweight = value.toDouble
    }
    if(key.equals("adaptive.reweighting.sample.ratio")){
      spc.adaptiveReweightingSampleRatio = value.toDouble
    }
    if(key.equals("data.per.iteration")){
      spc.batchSize = value.toDouble
    }
    spc
  }
}