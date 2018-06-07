# Association rule learning: a way to detect association between features.
<p align="center"><img src = "https://upload.wikimedia.org/wikipedia/commons/c/c3/Python-logo-notext.svg">
  
This repository was published to share a simple method to verify, based on some measurements, how a group of categorical features are related with others. Particulary this API allows to answer the question: **_is there an association between a set of features and a main feature (response variable)?_**.\
The use case of this implementations is the capability to create fraud contention rules that eventually migrate to the **SPSS Modeler routines**.\
The procedure's foundations are easy to understand, details can be found [**here**](https://en.wikipedia.org/wiki/Association_rule_learning).

The way to consume the Spark resources in this opportunity is through the Python API: **PySpark**. The reason why it's the ease of the Python language to write the program, as a _class_, that perform calculation over data represented on **pandas dataframes** using **lambda expresions**. The calculations inside the _class_ involves the **combinatorial operator** which is called from a Python module (_itertools_).

The measurements, early mentioned, are: **support** and **confidence**. Their interpretation is related to the concepts of **probability and the conditional probability**.
A main difference between this **development** and those made in Scala is that the modules and _objects_ come in one file, the **ARL.py**.
