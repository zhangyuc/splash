---
layout: page
title: Quick Start
permalink: /quickstart/
weight : 1
---

We provide a quick guide to developing stochastic algorithms via Splash. After reading this section, you may look at the [examples]({{site.baseurl}}/example/), or learn about the [Splash Machine Learning Package]({{site.baseurl}}/mlpackage/).

# Install Splash

To install Splash, you need to:

1. Download and install [Scala](http://www.scala-lang.org/index.html), [sbt](http://www.scala-sbt.org/index.html) and [Apache Spark](https://spark.apache.org/).
2. Download the [Splash jar file](https://github.com/zhangyuc/splash/blob/master/target/scala-2.10/splash-0.1.0.jar?raw=true) and put it in your project classpath.
3. Make the Splash jar file as a dependency when [submitting Spark jobs](http://spark.apache.org/docs/latest/submitting-applications.html).

For step 2-3, see how to compile and submit these [examples]({{site.baseurl}}/example/).

# Import Splash

When Splash is in your project classpath, you can write a self-contained Scala application using the Splash API. Besides importing Spark packages, you also need to import the Splash package in scala, by typing:

{% highlight scala %}
import splash.core._
{% endhighlight %}

# Create Dataset

The first step is to create a distributed dataset. Splash provides an abstraction called **Parametrized RDD**. The Parametrized RDD is very similar to the Resilient Distributed Dataset (RDD) of Spark. It can be created by a standard RDD:

{% highlight scala %}
val paramRdd = new ParametrizedRDD(rdd)
{% endhighlight %}

where `rdd` is the RDD that holds your dataset. 

# Set-up Data Processing Function

To execute the algorithm, set a data processing function `process` to the dataset by

{% highlight scala %}
paramRdd.setProcessFunction(process)
{% endhighlight %}

The `process` function is implemented by the user. It takes four objects as input: **a data element** from the dataset, **the weight** of this element, **the shared variable** shared across the dataset and **the local variable** associated with this element. The `process` function reads the input to perform some update on the variables. The capability of processing weighted elements is mandatory even if the original data is unweighted. This is because that Splash will automatically assign non-unit weights to the samples as a part of its parallelization strategy. For example, the algorithm for computing document statistics can be implemented as:

{% highlight scala %}
val process = (line: String, weight: Double, sharedVar: SharedVariableSet,  localVar: LocalVariableSet) => {
  sharedVar.add("lines", weight)
  sharedVar.add("words", weight * line.split(" ").length)
  sharedVar.add("characters", weight * line.length)
}
{% endhighlight %}

In the programming interface, all variables are stored as key-value pairs. The key must be a string. The value could be either a real number of an array of real numbers. In the above code, the shared variable is updated by the `add` method. If the algorithm needs to read a variable, use the `get` method:

{% highlight scala %}
val v1 = localVar.get(key)
val v2 = sharedVar.get(key)
{% endhighlight %}

The local variable can be updated by putting a new value:

{% highlight scala %}
localVar.set(key,value)
{% endhighlight %}

The shared variable must be updated by **transformations**. Splash provides three types of operators for transforming the shared variable: **add**, **delayed-add** and **multiply**. See the [Splash API]({{site.baseurl}}/api/) section for more information.

# Running the Algorithm

After setting up the processing function, call the `run()` method to start running the algorithm:

{% highlight scala %}
val spc = (new SplashConf).set("max.thread.number", 8)
paramRdd.run(spc)
{% endhighlight %}

so that every thread starts processing its local dataset and synchronize at the end. In the default setting, every thread takes a full pass over its local dataset by calling the `run()` method. You can change the amount of data to be processed by configuring the `spc` object. You can also take multiple passes over the dataset by multiple calls to the `run()` method.

# Output and Evaluation

After processing the dataset, you can output the result by:

{% highlight scala %}
val sharedVar = paramRdd.getSharedVariable()
println("Lines: " + sharedVar.get("lines").toLong)
println("words: " + sharedVar.get("words").toLong)
println("Characters: " + sharedVar.get("characters").toLong)
{% endhighlight %}

It is also possible to MapReduce the Parametrized RDD. For example, by calling

{% highlight scala %}
val loss = paramRdd.map(func).sum()
{% endhighlight %}

every element in the dataset is mapped by the `func` function. If `func` returns a real number, it will be aggregated across the dataset. This is useful for evaluating the performance of the algorithm. See [Splash API]({{site.baseurl}}/api/) for more options.


