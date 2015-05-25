package splash

import scala.collection.mutable._

class DeltaValue extends Serializable{
  var delta = 0.0
  var unweightedDelta = 0.0
  var prefactor = 1.0
  var sync = true
}

class DeltaValueArray extends Serializable{
  var array : Array[DeltaValue] = null
  var prefactor = 1.0
  var sync = true
}

class SharedVariableSet extends Serializable{
  var variable = new HashMap[String, Double]
  var delta = new HashMap[String, DeltaValue]
  var variableArray = new HashMap[String, Array[Double]]
  var deltaArray = new HashMap[String, DeltaValueArray]
  
  var delayedDelta : HashMap[(String, Int), Double] = null
  
  var batchSize : Long = 0
  
  var delayedAddShrinkFactor : Double = 1.0
  var delayedAddDefinedInEarlierIteration : Boolean = true
  
  var testLoss = -1.0
  
  // set operations are not recommended
  def set(key:String, value:Double) {
    variable.put(key, value)
  }
  
  def setArray(key:String, value:Array[Double]){
    if(!variableArray.contains(key)){
      variableArray(key) = new Array[Double](value.length)
    }
    val varArray = variableArray(key)
    for(i <- 0 until varArray.length){
      varArray(i) = value(i)
    }
  }
  
  def get(key:String) = {
    if(delta.contains(key)){
      val dv = delta(key)
      dv.prefactor * variable.applyOrElse(key, (x:Any) => 0.0) + dv.delta + dv.unweightedDelta
    }
    else{
      variable.applyOrElse(key, (x:Any) => 0.0)
    }
  }
  
  def declareArray(key:String, length:Int){
    val deltaValueArrayObject = new DeltaValueArray
    deltaValueArrayObject.array = new Array[DeltaValue](length)
    for(i <- 0 until length){
      deltaValueArrayObject.array(i) = new DeltaValue
    }
    deltaArray.put(key, deltaValueArrayObject)
  }
  
  def dontSync(key:String){
    delta(key).sync = false
  }
  
  def dontSyncArray(key:String){
    deltaArray(key).sync = false
  }
  
  private def refreshDeltaArrayElementPrefactor(dv: DeltaValue, new_factor:Double){
    if(dv.prefactor != new_factor){
      dv.delta *= new_factor / dv.prefactor
      dv.unweightedDelta *= new_factor / dv.prefactor
      dv.prefactor = new_factor
    }
  }
  
  def getArray(key:String) = {
    val varArray = variableArray.applyOrElse(key, (x:Any) => null)
    if(deltaArray.contains(key)){
      val deltaValueObject = deltaArray(key)
      val returnArray = new Array[Double](deltaValueObject.array.length)
      for(i <- 0 until deltaValueObject.array.length){
        val dv = deltaValueObject.array(i)
        refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
        if(varArray != null){
          returnArray(i) = dv.prefactor * variableArray(key)(i) + dv.delta + dv.unweightedDelta
        }
        else{
          returnArray(i) = dv.delta + dv.unweightedDelta
        }
      }
      returnArray
    }
    else{
      if(varArray != null) varArray.clone()
      else null
    }
  }
  
  def getArrayElement(key:String, index:Int) = {
    val varArray = variableArray.applyOrElse(key, (x:Any) => null)
    val varArrayElem = {
      if(varArray != null) varArray(index)
      else 0.0
    }
    if(deltaArray.contains(key)){
      val deltaValueObject = deltaArray(key)
      val dv = deltaValueObject.array(index)
      refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
      deltaValueObject.prefactor * varArrayElem + dv.delta + dv.unweightedDelta
    }
    else{
      varArrayElem
    }
  }
  
  def add(key:String, value:Double) {
    if(testLoss < 0){
      if(delta.contains(key)){
        val dv = delta(key)
        dv.delta += value
      }
      else{
        val dv = new DeltaValue
        dv.delta = value
        delta.put(key, dv)
      }
    }
    else{
      testLoss += value * value
    }
  }
  
  def addArray(key:String, value:Array[Double]){
    if(testLoss < 0){
      if(deltaArray.contains(key)){
        val deltaValueObject =deltaArray(key)
        val deltaValueArray = deltaValueObject.array
        for(i <- 0 until deltaValueArray.length){
          val dv = deltaValueArray(i)
          refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
          dv.delta += value(i)
        }
      }
      else{
        declareArray(key, variableArray(key).length)
        val deltaValueArray = deltaArray(key).array
        for(i <- 0 until deltaValueArray.length){
          deltaValueArray(i).delta = value(i)
        }
      }
    }
    else{
      for(i <- 0 until value.length){
        testLoss += value(i) * value(i)
      }
    }
  }
  
  def addArrayElement(key:String, index: Int, value:Double){
    if(testLoss < 0){
      if(deltaArray.contains(key)){
        val deltaValueObject = deltaArray(key)
        val dv = deltaValueObject.array(index)
        refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
        dv.delta += value
      }
      else{
        declareArray(key, variableArray(key).length)
        val deltaValueArray = deltaArray(key).array
        deltaValueArray(index).delta = value
      }
    }
    else{
      testLoss += value * value
    }
  }
  
  private def insertDelayedDelta(key:String, index: Int, value:Double) {
    if(delayedDelta == null){
      delayedDelta = new HashMap[(String, Int), Double]
    }
    if(delayedDelta.contains((key,index))){
      delayedDelta.put((key,index), delayedDelta((key,index)) + value * delayedAddShrinkFactor)
    }
    else{
      delayedDelta.put((key,index), value * delayedAddShrinkFactor)
    }
  }
  
  def delayedAdd(key:String, value:Double) {
    insertDelayedDelta(key, -1, value)
  }
  
  def delayedAddArray(key:String, value:Array[Double]){
    for(i <- 0 until value.length){
      insertDelayedDelta(key, i, value(i))
    }
  }
  
  def delayedAddArrayElement(key:String, index: Int, value:Double) {
    insertDelayedDelta(key, index, value)
  }
  
  def multiply(key:String, value:Double)
  {
    val adjusted_value = {
      if(value == 0) 1e-16
      else value
    }
    if(delta.contains(key)){
      val dv = delta(key)
      dv.prefactor *= adjusted_value
      dv.delta *= adjusted_value
      dv.unweightedDelta *= adjusted_value
    }
    else{
      val dv = new DeltaValue
      dv.prefactor = adjusted_value
      delta.put(key, dv)
    }
  }
  
  def multiplyArray(key:String, value:Double){
    val adjusted_value = {
      if(value == 0) 1e-16
      else value
    }
    if(deltaArray.contains(key)){
      deltaArray(key).prefactor *= adjusted_value
    }
    else{
      declareArray(key, variableArray(key).length)
      deltaArray(key).prefactor = adjusted_value
    }
  }
  
  def executeDelayedAdd(ops : Array[((String,Int), Double)])
  {
    if(ops != null){
      for(operator <- ops){
        val key = operator._1._1
        val index = operator._1._2
        val value = operator._2
        
        if(index == -1){ // add to variable
          if(delta.contains(key)){
            val dv = delta(key)
            if(delayedAddDefinedInEarlierIteration) dv.unweightedDelta += value
            else dv.delta += value
          }
          else{
            val dv = new DeltaValue
            if(delayedAddDefinedInEarlierIteration) dv.unweightedDelta = value
            else dv.delta = value
            delta.put(key, dv)
          }
        }
        else{ // add to variable array
          if(deltaArray.contains(key)){
            val deltaValueObject = deltaArray(key)
            val dv = deltaValueObject.array(index)
            refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
            if(delayedAddDefinedInEarlierIteration) dv.unweightedDelta += value
            else dv.delta += value
          }
          else{
            declareArray(key, variableArray(key).length)
            val deltaValueArray = deltaArray(key).array
            if(delayedAddDefinedInEarlierIteration) deltaValueArray(index).unweightedDelta = value
            else deltaValueArray(index).delta = value
          }
        }
      }
    }
  }
  
  def setDelayedDelta(ops : Array[((String, Int), Double)]){
    if(ops != null){
      if(delayedDelta != null){
        delayedDelta.clear()
      }
      else{
        delayedDelta = new HashMap[(String, Int), Double]
      }
      for(elem <- ops){
        delayedDelta.put(elem._1, elem._2)
      }
    }
  }
  
  def clearDelayedDelta(){
    if(delayedDelta != null){
      delayedDelta.clear()
    }
  }
  
  def exportDelayedDelta() = {
    if(delayedDelta == null){
      null
    }
    else{
      val ops = delayedDelta.toArray
      delayedDelta.clear()
      ops
    }
  }
  
  def refreshAllDeltaArrayElementPrefactor()
  {
    for(pair <- deltaArray){
      val dvArray = pair._2.array
      for(i <- 0 until dvArray.length){
        val dv = dvArray(i)
        refreshDeltaArrayElementPrefactor(dv, pair._2.prefactor)
      }
    }
  }
  
  
  def updateByProposal(prop:Proposal, weight : Double){
    for( pair <- prop.delta ){
      val key = pair._1
      variable.put(key, pair._2.prefactor * variable.applyOrElse(key, (x:Any) => 0.0) + pair._2.delta )
    }
    
    for( pair <- prop.deltaArray ){
      val key = pair._1
      if(variableArray.contains(key)){
        val varArray = variableArray(key)
        if(pair._2.prefactor == 1){
          for(i <- 0 until varArray.length){
            varArray(i) = varArray(i) + pair._2.array(i)
          }
        }
        else{
          for(i <- 0 until varArray.length){
            varArray(i) = pair._2.prefactor * varArray(i) + pair._2.array(i)
          }
        }
      }
      else{
        val varArray = new Array[Double](pair._2.array.length)
        for(i <- 0 until varArray.length){
          varArray(i) = pair._2.array(i)
        }
        variableArray.put(key, varArray)
      }
    }
    delta.clear()
    deltaArray.clear()
  }
  
  def exportProposal(weight : Double) = {
    val prop = new Proposal
    
    for(pair <- delta){
      if(pair._2.sync){
        val pdv = new ProposalDeltaValue
        pdv.prefactor = pair._2.prefactor
        pdv.delta = pair._2.delta / weight + pair._2.unweightedDelta 
        prop.delta.put(pair._1, pdv)
      }
      else{
        val key = pair._1
        variable.put(key, pair._2.prefactor * variable.applyOrElse(key, (x:Any) => 0.0) + pair._2.delta / weight + pair._2.unweightedDelta  )
      }
    }
    
    for(pair <- deltaArray){
      if(pair._2.sync){
        val pdva = new ProposalDeltaValueArray
        pdva.prefactor = pair._2.prefactor
        pdva.array = new Array[Double](pair._2.array.length)
        for(i <- 0 until pair._2.array.length){
          val dv = pair._2.array(i)
          refreshDeltaArrayElementPrefactor(dv, pair._2.prefactor)
          pdva.array(i) = dv.delta / weight + dv.unweightedDelta
        }
        prop.deltaArray.put(pair._1, pdva)
      }
      else{
        val key = pair._1
        if(variableArray.contains(key)){
          val varArray = variableArray(key)
          for(i <- 0 until varArray.length){
            val dv = pair._2.array(i)
            varArray(i) = pair._2.prefactor * varArray(i) + dv.delta / weight + dv.unweightedDelta
          }
        }
        else{
          val varArray = new Array[Double](pair._2.array.length)
          for(i <- 0 until varArray.length){
            val dv = pair._2.array(i)
            varArray(i) = dv.delta / weight + dv.unweightedDelta
          }
          variableArray.put(key, varArray)
        }
      }
    }
    prop
  }
}