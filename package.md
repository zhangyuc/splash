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

val weights = (new splash.optimization.StochasticGradientDescent())
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

# Collaborative Filtering for Personalized Recommendation

The **splash.recommendation** package implements the stochastic collaborative filtering algroithm for personalized implementation. The algorithm can be significantly faster than [MLlib's batch implementation](http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html). To use the package, the dataset should be an instance of `RDD[UserRating]`. The `UserRating` takes the form:

{% highlight scala %}

class UserRating(initUser : Int, initRatings : Array[(Int,Double)]) extends Serializable{
  var userId = initUser
  var ratings : Array[(Int,Double)] = initRatings
}

{% endhighlight %}

In the `UserRating` data structure, `userId` is the ID of the user. The array `ratings` is a list of item-rating pairs. To train a personalized recommendation model, run the code:

{% highlight scala %}

val matrixFactorizationModel = (new splash.recommendation.CollaborativeFiltering())
  .setRank(10)
  .setRegParam(0.02)
  .setNumIterations(20)
  .setStepSize(0.1)
  .train(ratings)

{% endhighlight %}

The method `train` returns a [MatrixFactorizationModel](https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/mllib/recommendation/MatrixFactorizationModel.html) object. Given this object, the user can make future recommendations. You can set the following parameters:

- **rank**: the algorithm approximates the user-rating matrix by a low-rank matrix whose rank is given by `rank`.
- **regParam**: the L2-regularization parameter for matrix factorization.
- **numIterations**: the number of rounds that the algorithm runs and synchronizes. 
- **stepSize**: a scalar value denoting the stepsize of the stochastic algorithm. 
- **dataPerIteration**: the proportion of local data processed in each iteration. The default value is `1.0`. By choosing a smaller proportion, the algorithm will synchronize more frequently or terminate more quickly.
- **maxThreadNum**: the maximum number of threads to run. The default value is equal to the number of Parametrized RDD partitions.
- **autoThread**: if the value is `true`, then the number of parallel threads will be chosen automatically by the system but is always bounded by **maxThreadNum**. Otherwise, the number of parallel threads will be equal to **maxThreadNum**.

<br>

# Collapsed Gibbs Sampling for Topic Modelling

The **splash.clustering** package implements the Collapsed Gibbs Sampling algorithm for fitting the [Latent Dirichlet Allocation (LDA)](http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation) model. To use the package, the dataset should be an instance of `RDD[(docId, Array[wordToken])]`. The `docId` is the ID of the document, the `wordToken` represents a word token in the document. It takes the form:

{% highlight scala %}

class WordToken(initWordId : Int, initWordCount : Int, initTopicId : Array[Int]) extends Serializable {
  var wordId = initWordId
  var wordCount = initWordCount
  var topicId = initTopicId 
}

{% endhighlight %}

In the `WordToken` data structure, `wordId` is the ID of the word. `wordCount` is the frequency of this word in the document. `topicId` is a list of topics that are assigned to this word. If the model has never been trained, these topics should be assigned by random indices. Otherwise the topics might be initialized by an earlier training result. Then call the `train` method to start running the algorithm:

{% highlight scala %}

val model = new splash.clustering.LDA()
val corpusWithNewTopics = model.setNumTopics(100).setAlphaBeta((0.1,0.01)).setNumIterations(100).train(corpusWithOldTopics)
val topicsMatrix = model.topicsMatrix 

{% endhighlight %}

The `train` method returns an `RDD[(docId, Array[wordToken])]` object in which the topic of each word token has been resampled. The member `model.topicsMatrix` is the word-topic probability table. It maintains the probability that a particular topic generates a particular word. See the [LDA Example]({{site.baseurl}}/example/#lda-via-ml-package) for more details. You can set the following parameters:

- **numIterations**: the number of times that Collapsed Gibbs Sampling goes through the dataset.
- **numTopics**: the number of topics of the LDA model.
- **alphaBeta**: the [(alpha, beta) hyper-parameters](https://www.cs.princeton.edu/~blei/papers/BleiNgJordan2003.pdf) of the LDA model.


