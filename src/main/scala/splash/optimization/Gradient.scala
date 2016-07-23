package splash.optimization
import org.apache.spark.mllib.linalg.{Vectors,Vector,DenseVector,SparseVector}
import Util.{log1pExp,yax,dot}
import scalaxy.loops._
import scala.language.postfixOps

abstract class Gradient extends Serializable {
  /**
   * Request the weight indices that are useful in computing the gradient
   * or return null if all indices are requested
   *
   * @param data features for one data point
   *
   * @return indices: Array[Int]
   */
  def requestWeightIndices(data: Vector) : Array[Int]

  /**
   * Compute the gradient and the loss given the features of a single data point.
   *
   * @param data features for one data point
   * @param label label for this data point
   * @param weights weights/coefficients corresponding to features
   *
   * @return gradient: Vector and loss : Double
   */
  def compute(data: Vector, label: Double, weights: Vector): (Vector, Double)
}

abstract class BinaryLinearModelGradient extends Gradient {
  def requestWeightIndices(data: Vector): Array[Int] = {
    data match {
      case _ : DenseVector => null
      case sd : SparseVector => sd.indices
      case _ => throw new IllegalArgumentException(s"doesn't support ${data.getClass}.")
    }
  }
}

class LogisticGradient extends BinaryLinearModelGradient{
  def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val margin = -1.0 * dot(data, weights)
    val multiplier = (1.0 / (1.0 + math.exp(margin))) - label
    val loss = {
      if (label > 0) {
        log1pExp(margin)
      } else {
        log1pExp(margin) - margin
      }
    }
    (yax(multiplier, data), loss)
  }
}

class MultiClassLogisticGradient(numClasses: Int) extends Gradient{
  def requestWeightIndices(data: Vector): Array[Int] = {
    data match {
      case _ : DenseVector => throw new IllegalArgumentException(s"data only supports sparse vector but got type ${data.getClass}.")
      case sd : SparseVector => {
        val sdIndices = sd.indices
        val n = sdIndices.length
        val N = data.size
        val indices = new Array[Int](n*(numClasses-1))
        for(i <- 0 until numClasses-1 optimized){
          var j = 0
          var base = i*n
          var Base = i*N
          while(j < n){
            indices(base+j) = Base + sdIndices(j)
            j += 1
          }
        }
        indices
      }
      case _ => throw new IllegalArgumentException(s"doesn't support ${data.getClass}.")
    }
  }

  def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    // marginY is margins(label - 1) in the formula.
    var marginY = 0.0
    var maxMargin = Double.NegativeInfinity
    var maxMarginIndex = 0

    val sparseData = data.asInstanceOf[SparseVector]
    val sparseWeight = weights.asInstanceOf[SparseVector]
    val dataIndices = sparseData.indices
    val dataValues = sparseData.values
    val weightIndices = sparseWeight.indices
    val weightValues = sparseWeight.values
    val n = dataIndices.length
    val deltaValues = new Array[Double](n*(numClasses-1))

    val margins = Array.tabulate(numClasses - 1) { i =>
      var margin = 0.0
      var base = i * n
      var j = 0
      while(j < n){
        margin += dataValues(j) * weightValues(base + j)
        j += 1
      }
      if (i == label.toInt - 1) marginY = margin
      if (margin > maxMargin) {
        maxMargin = margin
        maxMarginIndex = i
      }
      margin
    }
    val sum = {
      var temp = 0.0
      if (maxMargin > 0) {
        for (i <- 0 until numClasses - 1 optimized) {
          margins(i) -= maxMargin
          temp += (if (i == maxMarginIndex) {
            math.exp(-maxMargin)
          } else {
            math.exp(margins(i))
          })
        }
      } else {
        for (i <- 0 until numClasses - 1 optimized) {
          temp += math.exp(margins(i))
        }
      }
      temp
    }

    for (i <- 0 until numClasses - 1 optimized) {
      val multiplier = math.exp(margins(i)) / (sum + 1.0) - {
        if (label != 0.0 && label == i + 1) 1.0 else 0.0
      }
      var j = 0
      var base = i * n
      while(j < n){
        deltaValues(base+j) = multiplier * dataValues(j)
        j += 1

      }
    }
    var loss = if (label > 0.0) math.log1p(sum) - marginY else math.log1p(sum)
    if (maxMargin > 0) {
      loss += maxMargin
    }
    (Vectors.sparse(n*(numClasses - 1), weightIndices, deltaValues), loss)
  }
}

class HingeGradient extends BinaryLinearModelGradient {
  def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val dotProduct = dot(data, weights)
    val labelScaled = 2 * label - 1.0
    if (labelScaled * dotProduct < 1.0) {
      (yax(-labelScaled, data), 1.0 - labelScaled * dotProduct)
    } else {
      (Vectors.sparse(weights.size, Array.empty, Array.empty), 0.0)
    }
  }
}

class LeastSquaresGradient extends BinaryLinearModelGradient {
  def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val diff = dot(data, weights) - label
    val loss = diff * diff / 2.0
    val gradient = data.copy
    (yax(diff, gradient), loss)
  }
}
