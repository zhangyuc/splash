package splash.recommendation
import java.{util => ju}
import org.apache.spark.rdd.RDD
import splash.core._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import scala.util.Random
import scala.collection.mutable.ListBuffer
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import java.{util => ju}
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import scalaxy.loops._
import scala.language.postfixOps

class UserRating(initUser : Int, initRatings : Array[(Int,Double)], initValidation: Array[(Int,Double)] = Array[(Int,Double)]()) extends Serializable{
  var userId = initUser
  var ratings : Array[(Int,Double)] = initRatings
  var validation : Array[(Int,Double)] = initValidation
}

class CollaborativeFiltering {
  var iters = 10
  var stepsize = 1.0
  var dataPerIteration = 1.0
  var maxThreadNum = 0
  var rank = 0
  var regParam = 0.01
  var autoThread = true
  var process : (UserRating, Double, SharedVariableSet, LocalVariableSet ) => Unit = null
  var evalLoss : (UserRating, SharedVariableSet, LocalVariableSet ) => Double = null
  var printDebugInfo = false

  /*
   * start running the AdaGrad SGD algorithm.
   */
  def train(data: RDD[UserRating]) = {
    val numPartitions = data.partitions.length
    val paramRdd = new ParametrizedRDD(data.repartition(numPartitions))
    if(math.ceil(dataPerIteration).toInt > 1){
      paramRdd.duplicateAndReshuffle( math.ceil(dataPerIteration).toInt )
    }
    val n_train = data.map(x => x.ratings.length).sum()
    val n_test = data.map(x => x.validation.length).sum()
    val num_of_item = data.map( x => { var max = 0; for(r <- x.ratings) max = math.max(max, r._1); max} ).max()
    setProcessFunction()
    setEvalFunction()

    if(printDebugInfo){
      println("num_of_item = " + num_of_item)
      println("instance for training = " + n_train)
      println("instance for testing = " + n_test)
    }

    val rank = this.rank
    paramRdd.foreachSharedVariable((sharedVar: SharedVariableSet ) => {
      val rnd = new Random(1)
      for(item_id <- 1 until num_of_item + 1)
      {
        val vi = new Array[Double](rank + 1)
        var sum = 0.0
        for(i <- 0 until rank optimized){
          val v = math.abs(rnd.nextGaussian())
          vi(i) = v
          sum += v * v
        }
        sum = math.sqrt(sum)
        for(i <- 0 until rank optimized) vi(i) = vi(i) / sum
        sharedVar.setArray("I" + item_id, vi)
      }
    })
    paramRdd.process_func = this.process
    paramRdd.evaluate_func = this.evalLoss

    val spc = (new SplashConf).set("data.per.iteration", math.min(1, dataPerIteration)).set("max.thread.num", this.maxThreadNum).set("auto.thread", this.autoThread)
    for( i <- 0 until this.iters ){
      paramRdd.run(spc)
      if(printDebugInfo){
        val loss = paramRdd.map(evalLoss).sum() / n_test
        println("%5.3f\t%5.8f".format(paramRdd.totalTimeEllapsed, loss))
      }
    }

    val itemVec = data.context.parallelize(paramRdd.mapSharedVariable(sharedVar => {
      val vecs = new ListBuffer[(Int, Array[Double])]
      for(key <- sharedVar.variableArray.keySet){
        if(key.startsWith("I")){
          val index = key.substring(1).toInt
          val value = sharedVar.getArray(key)
          vecs.append((index,value))
        }
      }
      vecs.toArray
    }).first(), numPartitions)

    val userVec = paramRdd.mapSharedVariable(sharedVar => {
      val vecs = new ListBuffer[(Int, Array[Double])]
      for(key <- sharedVar.variableArray.keySet){
        if(key.startsWith("U")){
          val index = key.substring(1).toInt
          val value = sharedVar.getArray(key)
          vecs.append((index,value))
        }
      }
      vecs.toArray
    }).flatMap(x => x.iterator)

    new MatrixFactorizationModel(rank, userVec, itemVec)
  }

  private def setProcessFunction(): Unit = {
    val rank = this.rank
    val stepsize = this.stepsize
    val regParam = this.regParam

    this.process = (entry: UserRating, weight: Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
      val user_id = entry.userId
      val ratings = entry.ratings
      val n = ratings.length

      // re-compute the user's latent vector
      val vis = new Array[Array[Float]](n)
      for(i <- 0 until n optimized){
        val vi = sharedVar.getFloatArray("I" + ratings(i)._1)
        vis(i) = vi
      }

      val normalEquation = new NormalEquation(rank)
      for(i <- 0 until n optimized){
        val rating = ratings(i)
        val vi = vis(i)
        val short_vi = new Array[Float](rank)
        var j = 0
        while(j < rank){
          short_vi(j) = vi(j)
          j += 1
        }
        normalEquation.add(short_vi, rating._2)
      }
      val vu = {
        if(n > 0) (new CholeskySolver()).solve(normalEquation, regParam * ratings.length)
        else new Array[Float](rank)
      }
      sharedVar.setArray("U"+user_id, vu)

      // update the items' latent vector
      for(i <- 0 until n optimized){
        val rating = ratings(i)
        val vi = vis(i)
        val vi_delta = new Array[Double](rank+1)

        var vuvi = 0.0
        var j = 0
        while(j < rank){
          vuvi += vu(j) * vi(j)
          j += 1
        }
        val scalar = rating._2 - vuvi

        var visum = 0.0
        j = 0
        while(j < rank){
          val delta = scalar * vu(j) - regParam * vi(j)
          visum += delta * delta
          vi_delta(j) = delta
          j += 1
        }
        vi_delta(rank) = weight * visum / rank
        val alpha = stepsize * weight / math.sqrt(vi(rank) + vi_delta(rank))
        j = 0
        while(j < rank){
          vi_delta(j) *= alpha
          j += 1
        }
        sharedVar.addArray("I" + rating._1, vi_delta)
      }
    }
  }

  private def setEvalFunction(): Unit = {
    val rank = this.rank
    val stepsize = this.stepsize
    val regParam = this.regParam

    this.evalLoss = (entry: UserRating, sharedVar : SharedVariableSet,  localVar: LocalVariableSet ) => {
      val user_id = entry.userId
      val ratings = entry.ratings
      val n = ratings.length

      // re-compute the user's latent vector
      val vis = new Array[Array[Float]](n)
      for(i <- 0 until n optimized){
        val vi = sharedVar.getFloatArray("I" + ratings(i)._1)
        vis(i) = vi
      }

      val normalEquation = new NormalEquation(rank)
      for(i <- 0 until n optimized){
        val rating = ratings(i)
        val vi = vis(i)
        val short_vi = new Array[Float](rank)
        var j = 0
        while(j < rank){
          short_vi(j) = vi(j)
          j += 1
        }
        normalEquation.add(short_vi, rating._2)
      }
      val vu = {
        if(n > 0) (new CholeskySolver()).solve(normalEquation, regParam * ratings.length)
        else new Array[Float](rank)
      }

      var loss = 0.0
      for(rating <- entry.validation){
        val y = rating._2
        val vi = sharedVar.getFloatArray("I" + rating._1)

        var i = 0
        var vuvi = 0.0
        while(i < rank){
          vuvi += vu(i) * vi(i)
          i += 1
        }
        val scalar = y - vuvi
        loss += scalar * scalar
      }
      loss
    }
  }

  /*
   * set the number of rounds that SGD runs and synchronizes.
   */
  def setNumIterations(iters: Int) = {
    this.iters = iters
    this
  }

  /*
   * set a scalar value denoting the stepsize of stochastic gradient descent.
   * Although the stepsize of individual iterates are adaptively chosen by AdaGrad,
   * they will always be proportional to this parameter.
   */
  def setStepSize(stepsize: Double) = {
    this.stepsize = stepsize
    this
  }

  def setRank(rank: Int) = {
    this.rank = rank
    this
  }

  def setRegParam(regParam: Double) = {
    this.regParam = regParam
    this
  }

  /*
   * set the proportion of local data processed in each iteration. The default value is 1.0.
   * By choosing a smaller proportion, the algorithm will synchronize more frequently or
   * terminate more quickly.
   */
  def setDataPerIteration(dataPerIteration: Double) = {
    this.dataPerIteration = dataPerIteration
    this
  }

  /*
   * set the maximum number of threads to run. The default value is equal to the number of Parametrized RDD partitions.
   */
  def setMaxThreadNum(maxThreadNum: Int) = {
    this.maxThreadNum = maxThreadNum
    this
  }

  /*
   * if the value is true, then the number of parallel threads will be chosen
   * automatically by the system but is always bounded by maxThreadNum. Otherwise,
   * the number of parallel threads will be equal to maxThreadNum.
   */
  def setAutoThread(autoThread : Boolean) = {
    this.autoThread = autoThread
    this
  }

  /*
   * set if printing the debug info. The default value is false.
   */
  def setPrintDebugInfo(printDebugInfo : Boolean) = {
    this.printDebugInfo = printDebugInfo
    this
  }
}

class CholeskySolver{
  private val upper = "U"
  /**
   * Solves a least squares problem with L2 regularization:
   *
   *   min norm(A x - b)^2^ + lambda * norm(x)^2^
   *
   * @param ne a [[NormalEquation]] instance that contains AtA, Atb, and n (number of instances)
   * @param lambda regularization constant
   * @return the solution x
   */
  def solve(ne: NormalEquation, lambda: Double): Array[Float] = {
    val k = ne.k
    // Add scaled lambda to the diagonals of AtA.
    var i = 0
    var j = 2
    while (i < ne.triK) {
      ne.ata(i) += lambda
      i += j
      j += 1
    }
    val info = new intW(0)
    lapack.dppsv(upper, k, 1, ne.ata, ne.atb, k, info)
    val code = info.`val`
    assert(code == 0, s"lapack.dppsv returned $code.")
      val x = new Array[Float](k)
      i = 0
      while (i < k) {
        x(i) = ne.atb(i).toFloat
        i += 1
      }
      ne.reset()
      x
    }
}

class NormalEquation(val k: Int) extends Serializable {
  /** Number of entries in the upper triangular part of a k-by-k matrix. */
  val triK = k * (k + 1) / 2
  /** A^T^ * A */
  val ata = new Array[Double](triK)
  /** A^T^ * b */
  val atb = new Array[Double](k)

  private val da = new Array[Double](k)
  private val upper = "U"

  private def copyToDouble(a: Array[Float]): Unit = {
    var i = 0
    while (i < k) {
      da(i) = a(i)
      i += 1
    }
  }

  /** Adds an observation. */
  def add(a: Array[Float], b: Double, c: Double = 1.0): this.type = {
    require(c >= 0.0)
    require(a.length == k)
    copyToDouble(a)
    blas.dspr(upper, k, c, da, 1, ata)
    if (b != 0.0) {
      blas.daxpy(k, c * b, da, 1, atb, 1)
    }
    this
  }

  /** Merges another normal equation object. */
  def merge(other: NormalEquation): this.type = {
    require(other.k == k)
    blas.daxpy(ata.length, 1.0, other.ata, 1, ata, 1)
    blas.daxpy(atb.length, 1.0, other.atb, 1, atb, 1)
    this
  }

  /** Resets everything to zero, which should be called after each solve. */
  def reset(): Unit = {
    ju.Arrays.fill(ata, 0.0)
    ju.Arrays.fill(atb, 0.0)
  }
}
