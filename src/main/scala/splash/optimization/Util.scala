package splash.optimization
import org.apache.spark.mllib.linalg.{Vectors,Vector,DenseVector,SparseVector}

object Util {
  /**
   * y = a * x
   */
  def yax(a: Double, x: Vector): Vector = {
    x match {
      case (dx: DenseVector) => yax(a, dx)
      case (sx: SparseVector) => yax(a, sx)
      case _ =>
        throw new IllegalArgumentException(s"ax doesn't support ${x.getClass}.")
    }
  }

  /**
   * x = a * x
   */
  private def yax(a: Double, x: DenseVector): Vector = {
    val xValues = x.values
    val n = xValues.length
    val yValues = new Array[Double](n)
    
    var k = 0
    while (k < n) {
      yValues(k) = a * xValues(k)
      k += 1
    }
    Vectors.dense(yValues)
  }

  /**
   * x = a * x
   */
  private def yax(a: Double, x: SparseVector): Vector = {
    val xIndices = x.indices
    val xValues = x.values
    val n = xValues.length
    val yValues = new Array[Double](n)
    
    var k = 0
    while (k < n) {
      yValues(k) = a * xValues(k)
      k += 1
    }
    Vectors.sparse(n, xIndices, yValues)
  }

  /**
   * x = a * x
   */
  def dot(x: Vector, y: Vector): Double = {
    require(x.size == y.size,
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
      " x.size = " + x.size + ", y.size = " + y.size)
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
      case (sx: SparseVector, dy: DenseVector) =>
        dot(sx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        dot(sy, dx)
      case (sx: SparseVector, sy: SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  /**
   * dot(x, y)
   */
  private def dot(x: DenseVector, y: DenseVector): Double = {
    val n = x.size
    val xValue = x.values
    val yValue = y.values
    var sum = 0.0
    for(i <- 0 until n){
      sum += xValue(i) * yValue(i)
    }
    sum
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.size
    val nnzy = yIndices.size

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }
  
  /**
   * When `x` is positive and large, computing `math.log(1 + math.exp(x))` will lead to arithmetic
   * overflow. This will happen when `x > 709.78` which is not a very large number.
   * It can be addressed by rewriting the formula into `x + math.log1p(math.exp(-x))` when `x > 0`.
   *
   * @param x a floating-point value as input.
   * @return the result of `math.log(1 + math.exp(x))`.
   */
  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}