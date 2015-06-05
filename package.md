---
layout: page
title: ML Package
permalink: /mlpackage/
weight : 2
---

Splash implements classical stochastic algorithms for machine learning:

* TOC
{:toc}

# Stocahstic Gradient Descent

The **splash.optimization** package implements the [Adaptive SGD](http://www.magicbroom.info/Papers/DuchiHaSi10.pdf) algorithm. To use this package, the dataset should be label-feature pairs stored as `data: RDD[Double, Vector]`. The label should be {0,1,2,...} for classification problems. The `Vector` is defined in [the MLlib package](https://spark.apache.org/docs/1.0.0/api/scala/index.html#org.apache.spark.mllib.linalg.Vector). Running SGD is straightforward:

{% highlight scala %}

import splash.optimization._

val weights = (new StochasticGradientDescent())
  .setGradient(new LogisticGradient())
  .setNumIterations(20)
  .setStepSize(0.5)
  .optimize(data, initialWeights)

{% endhighlight %}

The `setGradient` method requires a **splash.optimization.Gradient** object as input. You may use Splash's pre-built Gradiant classes: `LogisticGradient`, `MultiClassLogisticGradient`, `HingeGradient` or `LeastSquaresGradient`; or implement your own Gradient class in the following format:

{% highlight scala %}

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

{% endhighlight %}

You can set the following paramters:

- **numIteration**: the number of rounds that SGD runs and synchronizes. 
- **stepSize**: a scalar value denoting the stepsize of stochastic gradient descent. Although the stepsize of individual iterates will be adaptively chosen by AdaGrad algorithm, they will always be proportional to this parameter.
- **dataPerIteration**: the proportion of local data processed in each iteration. The default value is `1.0`. By choosing a smaller proportion, the algorithm will synchronize more frequently or terminate more quickly.
- **maxThreadNum**: the maximum number of thread to run the algorithm. The default value is equal to the number of Parametrized RDD partitions.
- **setAutoThread**: if the value is `true`, then the number of parallel thread will be automatically chosen by the system but always bounded by **maxThreadNum**. Otherwise, the number of parallel thread will be equal to **maxThreadNum**.

# Collapsed Gibbs Sampling for LDA

The **splash.sampling** package implements the Collapsed Gibbs Sampling algorithm for learning the [Latent Dirichlet Allocation (LDA)](http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation) model. To use this package, the dataset should take the form `RDD[(docId, wordToken)]`. The `docId` is the ID of the document, the `wordToken` is represents a word token in this document, taking the form

{% highlight scala %}

class WordToken(initWordId : Int, initWordCount : Int, initTopicId : Int) extends Serializable {
  var wordId = initWordId
  var wordCount = initWordCount
  var topicId = initTopicId 
}

{% endhighlight %}

All IDs should be integers starting from zero. The Collapsed Gibbs Sampling algorithm resamples the `topicId` for each word. Running it is straightforward:

{% highlight scala %}

val corpusWithNewTopics = (new CollapsedGibbsSamplingForLDA)
  .setNumTopics(100)
  .setAlphaBeta((0.1,0.01))
  .setNumIterations(100)
  .sample(corpusWithOldTopics)

{% endhighlight %}

It returns an `RDD[(docId, wordToken)]` where the `topicId` of each word token has been resampled. You can set the following parameters:

- **numIteration**: the number of times that Collapsed Gibbs Sampling goes through the dataset.
- **numTopics**: the number of topics in the LDA model.
- **alphaBeta**: the [(alpha, beta) hyper-parameters](https://www.cs.princeton.edu/~blei/papers/BleiNgJordan2003.pdf in the LDA model.


