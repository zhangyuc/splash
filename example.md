---
layout: page
title: Examples
permalink: /example/
weight : 3
---

First, download the [Splash Example package](https://github.com/zhangyuc/splash/blob/master/examples/SplashExample.tar.gz?raw=true) and extract it at any directory. The source code locates at `/src/main/scala/`. The Splash library is included at `/lib/`, which puts Splash in your project classpath. To compile the code, `cd` into the directory where you extract the package and type:

{% highlight bash %}
sbt package
{% endhighlight %}

It generates a jar file at `./target/scala-2.10/splashexample.jar`. To run the code, submit this jar file as a Spark job:

{% highlight bash %}
YOUR_SPARK_HOME/bin/spark-submit --class ExampleName \
  --driver-memory 4G \
  --jars lib/splash-0.1.0.jar target/scala-2.10/splashexample.jar \
  data/covtype.txt > output.txt
{% endhighlight %}

Here, **YOUR_SPARK_HOME** should be replaced by the directory that Spark is installed; **ExampleName** should be replaced by `DocumentStatistics`, `SplashOptimization` or `LogisticRegression`, depending on the example you want to run. The file `splash-0.1.0.jar` is the Splash library and `splashexample.jar` is the compiled code to be executed. The argument `data/covtype.txt` stands for the location of the data file. The result is written to `output.txt`.

Hee is a list of examples:

* TOC
{:toc}

# Document Statistics

The Document Statistics example computes the word statistics of a document. The data processing function reads a weighted `line` form the document to update the number of `lines`, `words` and `characters` through a shared variable `sharedVar`. The `weight` argument tells the algorithm that this line appears `weight` times in the current observation.

{% highlight scala %}
import org.apache.spark.{SparkConf,SparkContext}
import splash.core.ParametrizedRDD

object DocumentStatistics {
  def main(args: Array[String]) {
    val path = args(0)
    val sc = new SparkContext(new SparkConf())
    
    val paramRdd = new ParametrizedRDD(sc.textFile(path))
    val sharedVar = paramRdd.setProcessFunction((line, weight, sharedVar, localVar) => {
      sharedVar.add("lines", weight)
      sharedVar.add("words", weight * line.split(" ").length)
      sharedVar.add("characters", weight * line.length)
    }).run().getSharedVariable()
    
    println("Lines: " + sharedVar.get("lines").toLong)
    println("words: " + sharedVar.get("words").toLong)
    println("Characters: " + sharedVar.get("characters").toLong)
  }
}
{% endhighlight %}


# Splash Optimization

Splash has a collection of pre-built algorithms for machine learning. One of them is the SGD algorithm for optimization. It provides an API similar to that of the [MLlib's optimization package](https://spark.apache.org/docs/latest/mllib-optimization.html). MLlib implements a mini-batch version of SGD but doesn't support the native sequential SGD. 

{% highlight scala %}
val weights = (new splash.optimization.StochasticGradientDescent())
  .setGradient(new splash.optimization.LogisticGradient())
  .setNumIterations(20)
  .optimize(rdd, initialWeights)
{% endhighlight %}

The above code creates a `StochasticGradientDescent` object, sets the gradient to be the logistic gradient and tells the algorithm to take 20 passes over the `rdd` dataset. The output of SGD is assigned to the vector `weights`. The full code in the [Splash Example package](https://github.com/zhangyuc/splash/blob/master/examples/SplashExample.tar.gz?raw=true) illustrates how this piece of code can be integrated with the data analytics pipeline of Spark.

On the [mnist8m dataset](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#mnist8m) (digit recognition, data size = 20GB), Splash's SGD is 25x faster than [MLlib's L-BFGS](https://spark.apache.org/docs/latest/mllib-optimization.html#l-bfgs) and 75x faster than [MLlib's mini-batch SGD (with manually tuned mini-batch size)](https://spark.apache.org/docs/latest/mllib-optimization.html#gradient-descent-and-stochastic-gradient-descent) for achieving the same logistic loss. All algorithms run on a 64-core cluster. 

<p align="center">
<img src="https://raw.githubusercontent.com/zhangyuc/splash/master/images/compare-with-lbfgs.png" width="350">
</p>


# SGD for Logistic Regression

We also provide a bare-hand Logistic Regression implementation using the Splash programming interface. It gives you a concrete idea of how the data processing function can be implemented:

{% highlight scala %}
val process = (elem: LabeledPoint, weight: Double, sharedVar : SharedVariableSet,  localVar: LocalVariableSet) => {
  val label = elem.label
  val features = elem.features.asInstanceOf[SparseVector]
  val xIndices = features.indices
  val xValues = features.values
  val n = xIndices.length
  
  sharedVar.add("t", weight)
  val t = sharedVar.get("t")
  val learningRate = sharedVar.get("learningRate")
  val regParam = sharedVar.get("regParam")
  
  // get weight vector w
  val w = sharedVar.getArrayElements("w", xIndices)
  
  // compute the inner product x * w
  var innerProduct = 0.0
  for(i <- 0 until n){
    innerProduct += xValues(i) * w(i)
  }
  val margin = - 1.0 * innerProduct
  val multiplier = (1.0 / (1.0 + math.exp(margin))) - label
  
  // compute the update
  val delta = new Array[Double](n)
  val stepsize = weight * learningRate / math.sqrt(t)
  for(i <- 0 until n){
    delta(i) = - stepsize * multiplier * xValues(i)
  }
  
  // update the weight vector
  sharedVar.multiplyArray("w", 1 - stepsize * regParam)
  sharedVar.addArrayElements("w", xIndices, delta)
}

{% endhighlight %}

The above code uses multiple operators on the shared variable. The **get** and **getArrayElements** method returns the variable values. The **multiplyArray** method scales the whole array by a constant. The **addArrayElements** method adds a `delta` vector to a subset of coordinates. See the full code base for more details.


