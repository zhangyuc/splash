
# What is Stochastic Algorithm?

**Stochastic algorithm** is a family of algorithms which processes large-scale dataset by sequentially processing its random samples. Because processing a single element is very cheap, the stochastic algorithm can perform many rounds of updates when the batch algorithm performs one update. This makes the stochastic algorithm an efficient approach to learning complicated models from large datasets. Typical examples of stochastic algorithm include:

-  [Stochastic Gradient Descent (SGD)](http://en.wikipedia.org/wiki/Stochastic_gradient_descent)
-  [Stochastic Dual Coordinate Ascent (SDCA)](http://www.jmlr.org/papers/volume14/shalev-shwartz13a/shalev-shwartz13a.pdf)
-  [Markov chain Monte Carlo (MCMC)](http://en.wikipedia.org/wiki/Markov_chain_Monte_Carlo)
-  [Gibbs Sampling](http://en.wikipedia.org/wiki/Gibbs_sampling)
-  [Stochastic Varitional Inference](http://www.columbia.edu/~jwp2128/Papers/HoffmanBleiWangPaisley2013.pdf)
-  [Expectation Propagation](http://research.microsoft.com/en-us/um/people/minka/papers/ep/minka-ep-uai.pdf).

# What is Splash?

**Splash** is a general-purppose framework for parallelizing stochastic algorithms on multi-node clusters. You can develop any stochastic algorithm using the Splash programming interface without concerning any detail about distribuetd computing. The parallelization is automatic and is very efficient. Splash is built on [Scala](http://www.scala-lang.org/) and [Apache Spark](https://spark.apache.org/), so that users can use it to process the [Resilient Distributed Datasets (RDD)](https://spark.apache.org/docs/latest/quick-start.html) of Spark.

On large-scale datasets, Splash is faster than the existing RDD-based data analytics tools. For example, to learn a 10-class logistic regression model on the [mnist8m dataset](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#mnist8m), The SGD algoritihm implemented via Splash is 25x faster than [MLlib's L-BFGS](https://spark.apache.org/docs/latest/mllib-optimization.html#l-bfgs) and 75x faster than [MLlib's mini-batch SGD](https://spark.apache.org/docs/latest/mllib-optimization.html#gradient-descent-and-stochastic-gradient-descent) for achieving the same logistic loss. All algorithms run on a 64-core cluster.

<p align="center">
<img src="https://raw.githubusercontent.com/zhangyuc/splash/master/images/compare-with-lbfgs.png" width="350">
</p>

Read the [Quick Start](/quickstart/) to start building your awesome stochastic algorithm!


