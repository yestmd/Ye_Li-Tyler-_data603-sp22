# A potential key to open the chamber of secrets of neurocognitive functions---Applying Thunder in neuroscience field

No doubt, neuroactivity is one of the fundamental pieces of the cognitive function. With the development of neuroscience research technical, researchers are getting better at recording high-resolution and real-time imaging of neural firing or cognitive behavior data. In this case, massive data sets are generated, which is flooding the researchers. Because of that, neuroscience research field have been recognized as an important field which could benefit from the big data method. However, there are still some general challenges to apply big data systems into neuroscience field. Like other fields, these challenges include data collection and sharing, data infrastructure construction, and data analysis methods [1]. Thunder, is a big data tools devolved by Howard Hughes Medical Institute (HHMI). This group published their first paper about Thunder tools at high impact journal, Nature Method at 2014 [2]. The usage of Thunder module may unlock the door to access the research of neuroscience field. 

## The main structure of Thunder module

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
## Conclusion



## Reference:
[1] Li, Xiang et al. Functional Neuroimaging in the New Era of Big Data. Genomics, proteomics & bioinformatics vol. 17,4 (2019): 393-401. doi:10.1016/j.gpb.2018.11.005

[2] Freeman J et al. Mapping brain activity at scale with cluster computing. Nat Methods. 2014 Sep;11(9):941-50. doi: 10.1038/nmeth.3041. Epub 2014 Jul 27. PMID: 25068736.
