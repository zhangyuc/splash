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

This generates a jar file at `./target/scala-2.10/splashexample.jar`. To run the code, submit this jar file as a Spark job:

{% highlight bash %}
YOUR_SPARK_HOME/bin/spark-submit --class ExampleName \
  --driver-memory 4G \
  --jars lib/splash-0.1.0.jar target/scala-2.10/splashexample.jar \
  [data files] > output.txt
{% endhighlight %}

Here, **YOUR_SPARK_HOME** should be replaced by the directory that Spark is installed; **ExampleName** should be replaced by the name of the example (see the following sections). The file `splash-0.1.0.jar` is the Splash library and `splashexample.jar` is the compiled code to be executed. The arguments `[data files]` should be replaced by the path of data files (see the following sections). The result is written to `output.txt`.

Here is a list of examples:

* TOC
{:toc}

# Document Statistics

The Document Statistics example computes the word statistics of a document. Please choose  `ExampleName = DocumentStatistics` and `[data files] = data/covtype.txt`. The processing function reads a weighted `line` from the document to update the number of `lines`, `words` and `characters` through a shared variable `sharedVar`. The `weight` argument tells the algorithm that this line appears `weight` times in the current observation.

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


# SGD for Logistic Regression

We provide a bare-hands Logistic Regression implementation using the Splash programming interface. This gives you a concrete idea of how the data processing function can be implemented. To run the code on the Covtype classification dataset, please choose  `ExampleName = LogisticRegression` and `[data files] = data/covtype.txt`.

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

The above code uses multiple operators on the shared variable. The **get** and **getArrayElements** methods return the variable values. The **multiplyArray** method scales the whole array by a constant. The **addArrayElements** method adds a `delta` vector to a subset of coordinates. See the full code base for more details.

# SGD via ML Package

Splash has a collection of pre-built algorithms for machine learning. One of them is the SGD algorithm for optimization. It provides an API similar to that of the [MLlib's optimization package](https://spark.apache.org/docs/latest/mllib-optimization.html). MLlib implements a mini-batch version of SGD but doesn't support the native sequential SGD. To run the code on the Covtype classification dataset, please choose  `ExampleName = SGDExample` and `[data files] = data/covtype.txt`.

{% highlight scala %}
val weights = (new splash.optimization.StochasticGradientDescent())
  .setGradient(new splash.optimization.LogisticGradient())
  .setNumIterations(20)
  .optimize(rdd, initialWeights)
{% endhighlight %}

The above code creates a `StochasticGradientDescent` object, sets the gradient to be the logistic gradient and tells the algorithm to take 20 passes over the `rdd` dataset. The output of SGD is assigned to the vector `weights`. The full code in the [Splash Example package](https://github.com/zhangyuc/splash/blob/master/examples/SplashExample.tar.gz?raw=true) illustrates how this piece of code can be integrated with the data analytics pipeline of Spark.

# LDA via ML Package

Another algorithm in the ML Package is the Collapsed Gibbs Sampling for training the LDA model. Its functionality is similar to the [Variational Inference of MLlib](http://spark.apache.org/docs/latest/mllib-clustering.html#latent-dirichlet-allocation-lda). Since the Collapsed Gibbs Samping is a stochastic algorithm, it converges faster than MLlib and sometimes converges to better solutions. To run the example on the NIPS article dataset, please choose  `ExampleName = LDAExample` and `[data files] = data/docword.nips.txt data/vocab.nips.txt`.

{% highlight scala %}
// train LDA using collapsed Gibbs sampling
val model = new splash.clustering.LDA
model.setNumTopics(numTopics).setAlphaBeta((alpha,beta)).setNumIterations(numIterations).train(corpus)

// view topics and their top words
val topicsMatrix = model.topicsMatrix
for(topicId <- 0 until numTopics){
  val wordProb = new ListBuffer[(Int,Double)]
  for(wordId <- 0 until vocSize) wordProb.append((wordId, topicsMatrix(wordId)(topicId)))
  val sortedWordProb = wordProb.sortWith( (a,b) => a._2 > b._2 )

  // output top words
  println()
  print("Topic " + (topicId+1) + ": ")
  for(i <- 0 until 20) print(vocab(sortedWordProb(i)._1) + " ")
}
{% endhighlight %}

The above code trains a LDA model on the `corpus`, which is an RDD containing the word distribution of 1,500 documents (see the source code for the construction of this data structure). The member `model.topicsMatrix` is the word-topic probability table. It maintains the probability that a particular topic generates a particular word. The last few lines of the code prints the top 20 words for each topic.

