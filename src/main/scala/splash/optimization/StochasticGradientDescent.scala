package splash.optimization
import org.apache.spark.mllib.linalg.{Vectors,Vector,DenseVector,SparseVector}
import org.apache.spark.rdd.RDD
import splash.core._

class StochasticGradientDescent {
  var gradient : Gradient = null
  var iters = 10
  var stepsize = 1.0
  var dataPerIteration = 1.0
  var maxThreadNum = 0
  var dimension = 0
  var autoThread = true
  var process : ((Double, Vector), Double, SharedVariableSet, LocalVariableSet ) => Unit = null
  var evalLoss : ((Double, Vector), SharedVariableSet, LocalVariableSet ) => Double = null
  var printDebugInfo = false
  
  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector) = {
    val numPartitions = data.partitions.length
    val paramRdd = new ParametrizedRDD(data.repartition(numPartitions))
    if(math.ceil(dataPerIteration).toInt > 1){
      paramRdd.duplicateAndReshuffle( math.ceil(dataPerIteration).toInt )
    }
    this.dimension = initialWeights.size
    val initVec = initialWeights
    val n = paramRdd.count()
    setProcessFunction()
    setEvalFunction()
    
    val dimension = this.dimension
    val preprocess = (sharedVar: SharedVariableSet ) => {
      if(initVec.isInstanceOf[DenseVector]){
        sharedVar.setArray("w", initVec.asInstanceOf[DenseVector].values)
      }
      else{
        val indices = initVec.asInstanceOf[SparseVector].indices
        val values = initVec.asInstanceOf[SparseVector].values
        sharedVar.declareArray("w", dimension)
        sharedVar.setArrayElements("w", indices, values)
      }
      sharedVar.declareArray("s", dimension)
    }
    paramRdd.foreachSharedVariable(preprocess)
    paramRdd.process_func = this.process
    paramRdd.evaluate_func = this.evalLoss
    
    val spc = (new SplashConf).set("data.per.iteration", math.min(1, dataPerIteration)).set("max.thread.num", this.maxThreadNum).set("auto.thread", this.autoThread)
    for( i <- 0 until this.iters ){
      paramRdd.run(spc)
      if(printDebugInfo){
        val loss = paramRdd.map(evalLoss).sum() / n
        println("%5.3f\t%5.8f\t".format(paramRdd.totalTimeEllapsed, loss) + paramRdd.lastIterationThreadNumber)
      }
    }
    Vectors.dense(paramRdd.getSharedVariable().getArray("w"))
  }
  
  private def setProcessFunction(){
    val gradientObj = this.gradient
    val dimension = this.dimension
    val stepsize = this.stepsize
    
    this.process = (entry: (Double, Vector), weight: Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
      val label = entry._1
      val data = entry._2
      
      // compute gradient
      val weightIndices = gradientObj.requestWeightIndices(data)
      val w = weightIndices match{
        case null => Vectors.dense(sharedVar.getArray("w"))
        case _ => Vectors.sparse(dimension, weightIndices, sharedVar.getArrayElements("w", weightIndices))
      }
      val delta = gradientObj.compute(data, label, w)._1
      
      // compute stepsize
      val actual_stepsize = stepsize * weight
      
      // compute update
      delta match{
        case denseDelta : DenseVector => {
          val values = denseDelta.values
          val n = values.length
          val values_square = new Array[Double](n)
          var k = 0
          while(k < n){
            values_square(k) = values(k) * values(k) * weight
            k += 1
          }
          sharedVar.addArray("s", values_square)
          val s = sharedVar.getArray("s")
          k = 0
          while(k < n){
            values(k) *= - actual_stepsize / math.sqrt(s(k))
            k += 1
          }
          sharedVar.addArray("w", values)
        }
        case sparseDelta : SparseVector => {
          val indices = sparseDelta.indices
          val values = sparseDelta.values
          val n = values.length
          val values_square = new Array[Double](n)
          var k = 0
          while(k < n){
            values_square(k) = values(k) * values(k) * weight
            k += 1
          }
          sharedVar.addArrayElements("s", indices, values_square)
          val s = sharedVar.getArrayElements("s", indices)
          k = 0
          while(k < n){
            values(k) *= - actual_stepsize / math.sqrt(s(k) + 1e-16)
            k += 1
          }
          sharedVar.addArrayElements("w", indices, values)
        }
      }
    }
  }
  
  private def setEvalFunction(){
    val gradientObj = this.gradient
    val dimension = this.dimension
    this.evalLoss = (entry: (Double, Vector), sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
      val label = entry._1
      val data = entry._2
      // compute gradient
      val weightIndices = gradientObj.requestWeightIndices(data)
      val w = weightIndices match{
        case null => Vectors.dense(sharedVar.getArray("w"))
        case _ => Vectors.sparse(dimension, weightIndices, sharedVar.getArrayElements("w", weightIndices))
      }
      gradientObj.compute(data, label, w)._2
    }
  }
  
  def setGradient(gradient: Gradient) = {
    this.gradient = gradient
    this
  }
  
  def setNumIterations(iters: Int) = {
    this.iters = iters 
    this 
  }
  
  def setStepSize(stepsize: Double) = {
    this.stepsize = stepsize
    this
  }
  
  def setDataPerIteration(dataPerIteration: Double) = {
    this.dataPerIteration = dataPerIteration
    this
  }
  
  def setMaxThreadNum(maxThreadNum: Int) = {
    this.maxThreadNum = maxThreadNum
    this
  }
  
  def setAutoThread(autoThread : Boolean) = {
    this.autoThread = autoThread
    this
  }
  
  def setPrintDebugInfo(printDebugInfo : Boolean) = {
    this.printDebugInfo = printDebugInfo
    this
  }
}