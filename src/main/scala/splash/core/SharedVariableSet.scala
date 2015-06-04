package splash.core

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

class SharedVariableBackup extends Serializable{
  var variable = new HashMap[String, Double]
  var variableArray = new HashMap[String, Array[Double]]
}

class SharedVariableSet extends Serializable{
  var variable = new HashMap[String, Double]
  var delta = new HashMap[String, DeltaValue]
  var variableArray = new HashMap[String, Array[Double]]
  var deltaArray = new HashMap[String, DeltaValueArray]
  var loss = 0.0
  
  var delayedDelta : HashMap[(String, Int), Double] = null
  var delayedAddShrinkFactor : Double = 1.0
  var backup = new HashMap[String, SharedVariableBackup]
  
  // variables for element counting
  var batchSize = 0
  var sampleIndex = 0
  
  // backup methods
  def createBackup(key:String){
    val bv = new SharedVariableBackup
    bv.variable = variable.clone()
    for(pair <- variableArray){
      bv.variableArray.put(pair._1, pair._2.clone())
    }
    backup.put(key, bv)
  }
  
  def restoreFromBackup(key:String){
    val bv = backup(key)
    variable = bv.variable.clone()
    variableArray.clear()
    for(pair <- bv.variableArray){
      variableArray.put(pair._1, pair._2.clone())
    }
  }
  
  def clearBackup(){
    backup.clear()
  }
  
  // set operators are not recommended
  def set(key:String, value:Double) {
    variable.put(key, value)
  }
  
  def setArray(key:String, value:Array[Double]){
    if(!variableArray.contains(key)){
      variableArray(key) = new Array[Double](value.length)
    }
    val varArray = variableArray(key)
    val n = varArray.length
    var i = 0
    while(i < n){
      varArray(i) = value(i)
      i += 1
    }
  }
  
  def setArrayElements(key:String, indices: Array[Int], values:Array[Double]){
    val varArray = variableArray(key)
    val n = varArray.length
    var i = 0
    while(i < n){
      varArray(indices(i)) = values(i)
      i += 1
    }
  }
  
  // other operators
  def declareArray(key:String, length:Int){
    val deltaValueArrayObject = new DeltaValueArray
    deltaValueArrayObject.array = new Array[DeltaValue](length)
    var i = 0
    while(i < length){
      deltaValueArrayObject.array(i) = new DeltaValue
      i += 1
    }
    deltaArray.put(key, deltaValueArrayObject)
  }
  
  def dontSync(key:String){
    if(delta.contains(key)){
      delta(key).sync = false
    }
  }
  
  def dontSyncArray(key:String){
    if(deltaArray.contains(key)){
      deltaArray(key).sync = false
    }
  }
  
  private def refreshDeltaArrayElementPrefactor(dv: DeltaValue, new_factor:Double){
    if(dv.prefactor != new_factor){
      dv.delta *= new_factor / dv.prefactor
      dv.unweightedDelta *= new_factor / dv.prefactor
      dv.prefactor = new_factor
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
  
  def getArray(key:String) = {
    val varArray = variableArray.applyOrElse(key, (x:Any) => null)
    if(deltaArray.contains(key)){
      val deltaValueObject = deltaArray(key)
      val n = deltaValueObject.array.length
      val returnArray = new Array[Double](n)
      var i = 0
      while(i < n){
        val dv = deltaValueObject.array(i)
        refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
        varArray match{
          case null => returnArray(i) = dv.delta + dv.unweightedDelta
          case _ => returnArray(i) = dv.prefactor * varArray(i) + dv.delta + dv.unweightedDelta
        }
        i += 1
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
  
  def getArrayElements(key:String, indices:Array[Int]) = {
    val values = new Array[Double](indices.length)
    val varArray = variableArray.applyOrElse(key, (x:Any) => null)
    val n = indices.length
    if(varArray != null){
      var i = 0
      while(i < n){
        values(i) = varArray(indices(i))
        i += 1
      }
    }
    if(deltaArray.contains(key)){
      val deltaValueObject = deltaArray(key)
      var i = 0
      while(i < n){
        val dv = deltaValueObject.array(indices(i))
        refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
        values(i) = deltaValueObject.prefactor * values(i) + dv.delta + dv.unweightedDelta
        i += 1
      }
    }
    values
  }
  
  def add(key:String, value:Double) {
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
  
  def addArray(key:String, values:Array[Double]){
    val n = values.length
    if(deltaArray.contains(key)){
      val deltaValueObject =deltaArray(key)
      val deltaValueArray = deltaValueObject.array
      var i = 0
      while(i < n){
        val dv = deltaValueArray(i)
        refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
        dv.delta += values(i)
        i += 1
      }
    }
    else{
      declareArray(key, variableArray(key).length)
      val deltaValueArray = deltaArray(key).array
      var i = 0
      while(i < n){
        deltaValueArray(i).delta = values(i)
        i += 1
      }
    }
  }
  
  def addArrayElement(key:String, index: Int, value:Double){
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
  
  def addArrayElements(key:String, indices: Array[Int], values: Array[Double]){
    val n = indices.length
    if(deltaArray.contains(key)){
      val deltaValueObject = deltaArray(key)
      var i = 0
      while(i < n){
        val dv = deltaValueObject.array(indices(i))
        refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
        dv.delta += values(i)
        i += 1
      }
    }
    else{
      declareArray(key, variableArray(key).length)
      val deltaValueArray = deltaArray(key).array
      var i = 0
      while(i < n){
        deltaValueArray(indices(i)).delta = values(i)
        i += 1
      }
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
            dv.unweightedDelta += value
          }
          else{
            val dv = new DeltaValue
            dv.unweightedDelta = value
            delta.put(key, dv)
          }
        }
        else{ // add to variable array
          if(deltaArray.contains(key)){
            val deltaValueObject = deltaArray(key)
            val dv = deltaValueObject.array(index)
            refreshDeltaArrayElementPrefactor(dv, deltaValueObject.prefactor)
            dv.unweightedDelta += value
          }
          else{
            declareArray(key, variableArray(key).length)
            val deltaValueArray = deltaArray(key).array
            deltaValueArray(index).unweightedDelta = value
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
  
  def updateVariableByProposal(prop:Proposal){
    for( pair <- prop.delta ){
      val key = pair._1
      variable.put(key, pair._2.prefactor * variable.applyOrElse(key, (x:Any) => 0.0) + pair._2.delta )
    }
    
    for( pair <- prop.deltaArray ){
      val key = pair._1
      if(variableArray.contains(key)){
        val varArray = variableArray(key)
        val n = varArray.length
        if(pair._2.prefactor == 1){
          var i = 0
          while(i < n){
            varArray(i) = varArray(i) + pair._2.array(i)
            i += 1
          }
        }
        else{
          var i = 0
          while(i < n){
            varArray(i) = pair._2.prefactor * varArray(i) + pair._2.array(i)
            i += 1
          }
        }
      }
      else{
        val n = pair._2.array.length
        val varArray = new Array[Double](n)
        var i = 0
        while(i < n){
          varArray(i) = pair._2.array(i)
          i += 1
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
      val n = pair._2.array.length
      if(pair._2.sync){
        val pdva = new ProposalDeltaValueArray
        pdva.prefactor = pair._2.prefactor
        pdva.array = new Array[Double](n)
        var i = 0
        while(i < n){
          val dv = pair._2.array(i)
          refreshDeltaArrayElementPrefactor(dv, pair._2.prefactor)
          pdva.array(i) = dv.delta / weight + dv.unweightedDelta
          i += 1
        }
        prop.deltaArray.put(pair._1, pdva)
      }
      else{
        val key = pair._1
        if(variableArray.contains(key)){
          val varArray = variableArray(key)
          var i = 0
          while(i < n){
            val dv = pair._2.array(i)
            varArray(i) = pair._2.prefactor * varArray(i) + dv.delta / weight + dv.unweightedDelta
            i += 1
          }
        }
        else{
          val varArray = new Array[Double](pair._2.array.length)
          var i = 0
          while(i < n){
            val dv = pair._2.array(i)
            varArray(i) = dv.delta / weight + dv.unweightedDelta
            i += 1
          }
          variableArray.put(key, varArray)
        }
      }
    }
    delta.clear()
    deltaArray.clear()
    prop
  }
}