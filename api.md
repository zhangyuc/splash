---
layout: page
title: Splash API
permalink: /api/
weight : 4
---

# Parametrized RDD Operators

The parametrized RDD provides a similar set of operations that are supported by Spark RDD. Since the parametrized RDD maintains local variables and shared variables, there are additional operations manipulating these data structures.

 Operation | Meaning
  --- | ---
this(*rdd*, *preservePartitions*) | Constructor. It returns a Parametrized RDD object constructed from `rdd`. There is an optional boolean argument `preservePartitions`. If `preservePartitions = true`, then the partitioning of `rdd` will be preserved. Otherwise, the original RDD will be repartitioned such that the number of partitions is equal to the number of available cores. The default value of `preservePartitions` is `false`.
map(*func*)       | Return a RDD formed by mapping each element by function `func`. The function takes the element and the associated local/shared variables as input
foreach(*func*)       | Process each element by function `func`. The function takes the element and the associated local/shared variables as input.
reduce(*func*) | Reduce all elements by function `func`. The function takes two elements as input and returns a single element as output.
mapSharedVariable (*func*)  | Return a RDD formed by mapping the shared variable set by function `func`.
foreachSharedVariable (*func*) | Process the shared variable set by function `func`.
reduceSharedVariable(*func*) | Reduce all shared variable sets by function `func`. The function takes two SharedVariableSet objects as input and returns a single SharedVariableSet object as output.
syncSharedVariable() | Synchronize the shared variable across all partitions. This operation often follows the execution of the above four operations. If the shared variables is manually changed but not synchronized, the change may not actually take effect.
getFirstSharedVariable() | Return the set of shared variables in the first partition.
getAllSharedVariable() | Return an array of the set of shared variables in all partitions.
setProcessFunction (*func*) | Set the data processing function. The function `func` takes an arbitrary element, the weight of the element and the associated local/shared variables. It performs update on the local/shared variables.
setLossFunction(*func*) | Set a loss function for the stochastic algorithm. The function `func` takes an element and the associated local/shared variables. It returns the loss incurred by this element. Setting a loss function for the algorithm is optional, but a reasonable loss function may help Splash choosing a better parallelization strategy.
run(*spc*) | Use the data processing function to process the dataset. `spc` is a StreamProcessContext object. It includes hyper-parameters for running the algorithm.
duplicateAndReshuffle(*n*) | Make `n-1` copies of every element and reshuffle them across partitions. This will enlarge the dataset by a factor of `n`. Parallel threads can reduce communication costs by processing a larger local dataset. If `n=1`, then the dataset is simply reshuffled.

# Shared Variable Set Operators

All shared variables are organized as a SharedVariableSet instance. There are operations for reading and writing the shared variable set. We list them in a table:

 Operation | Meaning
  --- | ---
get(*key*) | Return the value of the key. The initial value is 0.
add(*key*, *delta*) | Add `delta` to the value of the key.
delayedAdd(*key*, *delta*) | Same as `add`, but the operation will not be executed instantly. Instead, it will be executed at the next time the same element is processed. The delayed operation is useful for reversing a previous operation on the same element, or for passing information to the future.
multiply(*key*, *gamma*) | Multiply the value of the key by `gamma`.
declareArray(*key*, *length*) | Declare an array associated with the `key`. The `length` argument indicates the dimension of the array. The array has to be declared before manipulated. Generally speaking, manipulating an array of real numbers is faster than manipulating the same number of key-value pairs.
getArray(*key*) | Return the array associated with the key. It will return `null` if the array has not been declared.
getArrayElement(*key*, *ind*) | Return the array element with index `ind`. It will return 0 if the array has not been declared.
addArray(*key*, *delta*) | Add an array `delta` to the array associated with the key. 
addArrayElement (*key*, *ind*, *delta*) | Add `delta` to the array element with index `ind`.
delayedAddArray (*key*, *delta*) | The same as `addArray`, but the operation will not be executed until the next time the same element is processed.
delayedAddArrayElement (*key*, *delta*, *delta*) | The same as `addArrayElement`, but the operation will not be executed until the next time the same element is processed.
multiplyArray (*key*, *gamma*) | Multiply all elements of the array by a real number `gamma`. The computation complexity of this operation is **O(1)**, independent of the dimension of the array.
dontSync(*key*) | The system will not synchronize this variable at the end of the current iteration. It will improve the communication efficiency, but may cause unpredictable consistency issues. Don't register a variable as `dontSync` unless you are sure that it will never be used by other partitions. The variable will still be synchronized at later iterations.
dontSyncArray(*key*) | The same as `dontSync`, but the object is an array-type variable.

# Stream Process Context

The Stream Process Context allows the user setting customized properties for the algorithm execution. Given a Stream Process Context object `spc`, the properties are set by

	spc = spc.set(propertyName,propertyValue)

Here is a list of configurable properties:

Property Name | Default | Meaning
--- | :---: | ---
num.of.thread | 0 | The number of parallel threads for algorithm execution. If `num.of.thread = 0`, then the number of parallel thread is equal to the number of partitions.
num.of.group | 0 | Indicate the number of groups that Splash clusters the parallel threads. It is a system-level parameter. If `num.of.group = 0`, then the group number will be automatically chosen.
num.of.pass.over .local.data | 1.0 | Proportion of local data processed per one call of the `run` function. If this number is greater than 1, then each thread will take multiple passes over its local dataset before synchronization.
warmStart | true | If `warmStart = true`, then at the first time the `run` function is called, the system will not execute parallel processing immediately. It will begin by running the algorithm sequentially on a small fraction of data as a warm start. This feature can be turned off by setting `warmStart = false`.
