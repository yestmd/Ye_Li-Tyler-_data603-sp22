# Thunder--A potential key to open the chamber of secrets of neuroscience

No doubt, neuroactivity is one of the fundamental pieces of the cognitive function. With the development of neuroscience research technical, researchers are getting better at recording high-resolution and real-time imaging of neural firing or cognitive behavior data. In this case, massive data sets are generated, which is flooding the researchers. Because of that, neuroscience research field have been recognized as an important field which could benefit from the big data method. 

However, there are still some general challenges to use these large-scale data in neuroscience field, such as data collection and sharing, data infrastructure construction, and data analysis methods [1].  Apche Spark, or Spark is a essential analytics engine when handling large-scale data. It provides a possible solution for neuroscience researchers to handle their large experiment data. Running on Spark engine, Howard Hughes Medical Institute (HHMI) developed a big data toolbox, Thunder [2]. The usage of Thunder maybe a key of the chamber of secrets of neuroscience. 

## Spark, the foundation structure of Thunder

Spark, or Apache Spark is the widely used analytics engine designed to big data processing. As the following figure shown, the Spark applications include three components: Driver Program, Cluster Manager and Worker Node. Once SparkContext connected to Cluster Manager, Spark will grasp nodes in cluster environment and send your code to executors. With the code sent to executors, SparkContext than sends tasks to executors for computations. These structure set can run independently on cluster with its own executor without sharing data. 

![figure for spark](https://user-images.githubusercontent.com/54827137/165408041-b237e397-bab2-4ab7-90e2-d5fcc7d0e7e4.png)
                        *<p align="center"> Cited from https://spark.apache.org/docs/latest/cluster-overview.html</p>*

One of the advantages of Spark is the using of Resilient Distributed Dataset (RDD). This data format can be immutable split across a cluster. And then the split chunks can be computed parallelly in a batch process. Plus, with large memory’s help, the intermediate computation can be processed in the In-memory storage method for these split data. These features help Spark applications running under high speed when processing large-scale data, such as the data generated in neuroscience field.

By applying the powerful capability of Spark in processing big data, Thunder is developed as a Python tool sets integrated on Spark platform, which focuses on processing spatial or temporal data sets. 

## The main structure of Thunder toolbox

Thunder is established from a modular collection. The usage of Thunder currently focuses on image and time series data based on Python code. The core Thunder package is for defining data structures and read/write patterns. The main analysis modules are archived by related packages:

### *thunder-regression:* 
This package provides mass univariate regression method for the dataset with many response variables. The independently multiple variable vs single set of explanatory features is the characteristic of this kind dataset. The algorithms of this regression package highly integrated scikit-learn style. 

### *thunder-registration:* 
The thunder-registration package is design for registering video data captured from medical or neuroscience imaging experiment. In this package, the mainly algorithm is the cross-correlation, which is a measurement to calculate the similarity of two series. It provides a function to calculate the integer n-dimensional displacement between images and reference. 

### *thunder-factorization:* 
This package provides the algorithms for large-scale matrix factorization by applying PCA, ICA, NMF, SVD or other method. All these algorithms can be used locally from scikit-learn or parallelization via spark.

All these packages can work together, also can be used separately, or even with other tools in Python environment, such as pandas. 

## The neuroscience data types used in Thunder

When we handle neuroscience experiment, there are two major data types used by researchers, the images and the series data. In Thunder usage environment, the images data is treated as a collection of images. Video data can also be treated as images data that generated from different timestamps, such as the real-time fMRI brain scan imaging data. 

Besides, the series data type is a collection of one-dimensional records with a common index, for example, the time series data is one of the most important series data types generated from brain electrical activity scan. 

In Thunder, there is also a special data structure, named block. It is an intermediate data type between images and series. It can be used to represent the data with spatial and time feature. 

## Apply Thunder in Databricks environment

In this section, some sample image data will be run in Thunder moduel on Databricks platform. The Databricks is a web-based platform for running Spark. By developing the 2nd generation Tungsten engine, the Databricks platform push the Spark 2.0 work even faster. Databricks provide the community edition for free. In the community edition, a cluster can be created with around 16GB RAM and 2core CPU. 


![cluster name](https://user-images.githubusercontent.com/54827137/165428319-d1735328-84a1-4faf-9785-a889bd6d5301.png)



Before import Thunder package, we will need to install it and the showit package, which is used for display image data. 


![install thunder](https://user-images.githubusercontent.com/54827137/165434387-8e7f4bea-b1a4-4d91-8e0c-95752e5d52d9.png)


![install showit](https://user-images.githubusercontent.com/54827137/165418517-14c98671-7cdb-4445-92b5-aaf285242fe2.png)



After the Thunder package successfully installed, we can import it and the image display module. 



![import thunder](https://user-images.githubusercontent.com/54827137/165418664-61761e2c-d277-4ddf-a71f-538fc317ab6a.png)


![import showit](https://user-images.githubusercontent.com/54827137/165418884-2ba2f2c8-6c0e-4874-bf9d-3c09a8f7a2ae.png)



In the Thunder package, there are some example data that we can work with. In the Thunder package, we can load the data as images or series by using *images.fromexample()* or *series.fromexample()*. In the fromexample function, we can use *engine* to load image locally or using sc, for SparkContext in Spark. 



![local load](https://user-images.githubusercontent.com/54827137/165419679-60d3e308-e54a-45d7-8dc2-3f84af03582f.png)
![spark load](https://user-images.githubusercontent.com/54827137/165419683-61e93429-6333-4ad3-bf48-99e5d1de5937.png)



The example data, fish, is a 3D volume image data. 



![sample photo](https://user-images.githubusercontent.com/54827137/165420110-cc2b7345-1f5b-4a7a-bc81-297a6e71c400.png)


Next, we will use the function in the Thunder toolbox to do some image manipulating and compare the time spent between Spark mode and local mode.
The manipulating function we used here is *median_filter()*, *gaussian_filter()*, *max_projection()*.


The median filter uses a non-linear filter to remove the noise in the image. Gaussian filter uses a low pass filter to smooth the image. Both of functions are used at every image in the 3D volume. 



![median filter](https://user-images.githubusercontent.com/54827137/165427332-288f65a4-d2d7-4f3d-b054-3d2be4fc8a9e.png)
![gaussian filter](https://user-images.githubusercontent.com/54827137/165427355-66359efc-63e3-4431-805e-ea6fd2fcd84f.png)



The max projection is to compute the maximum projection along the z dimension. 


<img src="https://user-images.githubusercontent.com/54827137/165427369-0a698e66-1f22-4399-9bb2-f925ed30cb55.png" width="45%"/>


We load the image by using engine=sc and without sc, and run these manipulating function here. The time comsuption shown at Spark mode is much faster than local mode. 
(1.92 ms vs 22.6 ms; 1.77 ms vs 18.2 ms; 1.62 ms vs 17.9 ms)



computation with Spark engine

![sc mode](https://user-images.githubusercontent.com/54827137/165434598-a506baa8-0461-417e-a68d-b81dda555bff.png)


computation locally

![local mode](https://user-images.githubusercontent.com/54827137/165434619-aa7ba165-95bb-4a5a-a18b-b1d35f998185.png)




## Conclusion
In this paper, after introducing the basic concept of Apche Spark, we descripted the structure of Thunder toolbox and the data types that can be used in Thunder toolbox in neuroscience file. Next, we run the Thunder in the Databricks, a web-based platform to compare the time consumption between using Spark engine and without Spark engine. The result suggests a more than ten times shorter computation time when using Spark engine. Also, it is worth to point out that the Thunder is open source, and requires a minimal Python experience to use it, which give opportunity to more neuroscience researchers to use it to analysis their data. No doubt, with more and more knowledge of Python and RDD concept contributed to neuroscience community, the using of Thunder or other toolbox will be used in the future, which would help researchers to uncover the secret of neuroscience chamber. 

## Reference:
[[1] Li, Xiang et al. Functional Neuroimaging in the New Era of Big Data. Genomics, proteomics & bioinformatics vol. 17,4 (2019): 393-401. doi:10.1016/j.gpb.2018.11.005 ](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6943787/)

[[2] Freeman J et al. Mapping brain activity at scale with cluster computing. Nat Methods. 2014 Sep;11(9):941-50. doi: 10.1038/nmeth.3041. Epub 2014 Jul 27. PMID: 25068736.](https://pubmed.ncbi.nlm.nih.gov/25068736/)

[[3] figure cited from https://spark.apache.org/docs/latest/cluster-overview.html](https://spark.apache.org/docs/latest/cluster-overview.html)

