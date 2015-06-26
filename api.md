---
layout: page
title: Splash API
permalink: /api/
weight : 4
---

* TOC
{:toc}



# Parametrized RDD Operators

The Parametrized RDD provides a similar set of operators supported by Spark RDD. Since the Parametrized RDD maintains local variables and shared variables, there are operators manipulating the data structure.

 Operator | Meaning
  --- | ---
this(*rdd*) | Constructor. It returns a Parametrized RDD object constructed from `rdd`. The partitioning of original `rdd` will be preserved.
reshuffle() | Reshuffle all elements across partitions. If your original dataset has not been shuffled, this operation is recommended at the creation of the Parametrized RDD.
map(*func*)       | Return a RDD formed by mapping each element by function `func`. The function takes the element and the associated local/shared variables as input
foreach(*func*)       | Process each element by function `func`. The function takes the element and the associated local/shared variables as input.
reduce(*func*) | Reduce all elements by function `func`. The function takes two elements as input and returns a single element as output.
mapSharedVariable(*func*)  | Return a RDD formed by mapping the shared variable set by function `func`.
foreachSharedVariable(*func*) | Process the shared variable set by function `func`.
reduceSharedVariable(*func*) | Reduce all shared variable sets by function `func`. The function takes two SharedVariableSet objects as input and returns a single SharedVariableSet object as output.
syncSharedVariable() | Synchronize the shared variable across all partitions. This operation often follows the execution of the above four operations. If the shared variables is manually changed but not synchronized, the change may not actually take effect.
getSharedVariable() | Return the set of shared variables in the first partition.
getAllSharedVariables() | Return the set of shared variables in all partitions.
setProcessFunction(*func*) | Set the data processing function. The function `func` takes an arbitrary element, the weight of the element and the associated local/shared variables. It performs update on the local/shared variables.
setLossFunction(*func*) | Set a loss function for the stochastic algorithm. The function `func` takes an element and the associated local/shared variables. It returns the loss incurred by this element. Setting a loss function for the algorithm is optional, but a reasonable loss function may help Splash choosing a better degree of parallelism.
run(*spc*) | Use the data processing function to process the dataset. `spc` is a `SplashConf` object. It specifies the hyper-parameters that the system needs.
duplicateAndReshuffle(*n*) | Make `n-1` copies of every element and reshuffle them across partitions. This will enlarge the dataset by a factor of `n`. Parallel threads can reduce communication costs by passing a larger local dataset.
duplicate(*n*) | Make `n-1` copies of every element without reshuffling.

<br>

# Local Variable Operators

The local variables assocaited with a data element are organized as a LocalVariableSet instance. The supported operators are:

Operator | Meaning
  --- | ---
get(*key*) | Return the value of associated with the key. The value is 0 if the variable has never been set.
set(*key*, *value*) &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Set the variable indexed by `key` to be equal to `value`. 
toArray() | Convert the variable set to an array of key-value pairs.

<br>

# Shared Variable Operators

The shared variables are organized as a SharedVariableSet instance. The supported operators are:

 Operator | Meaning
  --- | ---
get(*key*) | Return the value of the key. The initial value is 0.
add(*key*, *delta*) | Add `delta` to the value of the key.
delayedAdd(*key*, *delta*) | Same as `add`, but the operation will not be executed instantly. Instead, it will be executed at the next time the same element is processed. The delayed operation is useful for reversing a previous operation on the same element, or for passing information to the future.
multiply(*key*, *gamma*) | Multiply the value of the key by `gamma`.
declareArray(*key*, *length*) | Declare an array associated with the `key`. The `length` argument indicates the dimension of the array. The array has to be declared before manipulated. Generally speaking, manipulating an array of real numbers is faster than manipulating the same number of key-value pairs.
getArray(*key*) | Return the array associated with the key. It will return `null` if the array has not been declared.
getArrayElement(*key*, *ind*) | Return the array element with index `ind`. It will return 0 if the array has not been declared.
getArrayElements(*key*, *inds*) | Return array elements with specified indices `inds: Array[Int]`.
addArray(*key*, *delta*) | Add `delta: Array[Double]` to the array associated with the key. 
addArrayElement(*key*, *ind*, *delta*) | Add `delta: Array[Double]` to the specified indices `inds: Array[Int]`. The dimenions of `delta` and `inds` should be equal.
addArrayElements(*key*, *inds*, *delta*) | Add `delta` to the array element with index `ind`.
delayedAddArray(*key*, *delta*) | The same as `addArray`, but the operation will not be executed until the next time the same element is processed.
delayedAddArrayElement(*key*, *delta*, *delta*) | The same as `addArrayElement`, but the operation will not be executed until the next time the same element is processed.
multiplyArray(*key*, *gamma*) | Multiply all elements of the array by a real number `gamma`. The computation complexity of this operation is **O(1)**, independent of the dimension of the array.
dontSync(*key*) | The system will not synchronize this variable at the next round of synchronization. It will improve the communication efficiency, but may cause unpredictable consistency issues. Don't register a variable as `dontSync` unless you are sure that it will never be used by other partitions. The `dontSync` declaration is only effective at the current iteration.
dontSyncArray(*key*) | The same as `dontSync`, but the object is an array.

<br>

# Splash Configurations

The Splash Configuration class allows the user customizing the algorithm execution. Given a SplashConf instance `spc`, the properties are set by

~~~
spc.set(propertyName,propertyValue)
~~~

Here is a list of configurable properties:

Property Name | &nbsp;Default&nbsp;Value&nbsp; | Meaning
--- | :---: | ---
max.thread.num | 0 | The maximum number of threads to run the algorithm. If `max.thread.num = 0`, then the maximum number of threads is the number of partitions.
auto.thread | true | If the value is `true`, then the system will automatically determine the number of threads to run the algorithm. Otherwise, the number of threads will be equal to the maximum number of threads.
data.per.iteration | 1.0 | Proportion of local data processed in each iteration. In each iteration, every local thread goes through `data.per.iteration` proportion of its partition, then all threads synchronize at the end of the iteration. This proportion cannot be greater than 1. If you want to take multiple passes over the dataset in one iteration, use `duplicate` or `duplicateAndReshuffle` to enlarge the dataset.

