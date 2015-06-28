---
layout: page
title: ML Package
permalink: /mlpackage/
weight : 2
---

The current version of Splash implements several classical stochastic algorithms for machine learning, including:

* TOC
{:toc}

# Stochastic Gradient Descent

The **splash.optimization** package implements the [AdaGrad SGD](http://www.magicbroom.info/Papers/DuchiHaSi10.pdf) algorithm. To use this package, the dataset should be label-feature pairs stored as `data: RDD[Double, Vector]`. The label should be {0,1,2,...} for classification problems. The `Vector` is defined in [the MLlib package](https://spark.apache.org/docs/1.0.0/api/scala/index.html#org.apache.spark.mllib.linalg.Vector). Call the `optimize` method to start running the algorithm:

{% highlight scala %}

import splash.optimization._

val weights = (new StochasticGradientDescent())
  .setGradient(new LogisticGradient())
  .setNumIterations(20)
  .setStepSize(0.5)
  .optimize(data, initialWeights)

{% endhighlight %}

The `setGradient` method requires a **splash.optimization.Gradient** object as input. You may use Splash's pre-built Gradient classes: `LogisticGradient`, `MultiClassLogisticGradient`, `HingeGradient` or `LeastSquaresGradient`; or implement your own Gradient class in the following format:

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

You can set the following parameters:

- **numIterations**: the number of rounds that SGD runs and synchronizes. 
- **stepSize**: a scalar value denoting the stepsize of stochastic gradient descent. Although the stepsize of individual iterates are adaptively chosen by AdaGrad, they will always be proportional to this parameter.
- **dataPerIteration**: the proportion of local data processed in each iteration. The default value is `1.0`. By choosing a smaller proportion, the algorithm will synchronize more frequently or terminate more quickly.
- **maxThreadNum**: the maximum number of threads to run. The default value is equal to the number of Parametrized RDD partitions.
- **autoThread**: if the value is `true`, then the number of parallel threads will be chosen automatically by the system but is always bounded by **maxThreadNum**. Otherwise, the number of parallel threads will be equal to **maxThreadNum**.

<br>

# Collapsed Gibbs Sampling for LDA

The **splash.sampling** package implements the Collapsed Gibbs Sampling algorithm for fitting the [Latent Dirichlet Allocation (LDA)](http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation) model. To use this package, the dataset should be an instance of `RDD[(docId, wordToken)]`. The `docId` is the ID of the document, the `wordToken` represents a word token in the document. It takes the form

{% highlight scala %}

class WordToken(initWordId : Int, initWordCount : Int, initTopicId : Int) extends Serializable {
  var wordId = initWordId
  var wordCount = initWordCount
  var topicId = initTopicId 
}

{% endhighlight %}

`docId`, `wordId` and `topicId` should be integers starting from zero. The Collapsed Gibbs Sampling algorithm resamples the `topicId` for each word. Call the `sample` method to start running the algorithm:

{% highlight scala %}

import splash.sampling._

val corpusWithNewTopics = (new CollapsedGibbsSamplingForLDA)
  .setNumTopics(100)
  .setAlphaBeta((0.1,0.01))
  .setNumIterations(100)
  .sample(corpusWithOldTopics)

{% endhighlight %}

The `sample` method returns an `RDD[(docId, wordToken)]` object in which the topic of each word token has been resampled. You can set the following parameters:

- **numIterations**: the number of times that Collapsed Gibbs Sampling goes through the dataset.
- **numTopics**: the number of topics of the LDA model.
- **alphaBeta**: the [(alpha, beta) hyper-parameters](https://www.cs.princeton.edu/~blei/papers/BleiNgJordan2003.pdf) of the LDA model.


