# Association rule learning: a way to detect association between features.
<p align="center"><img src = "https://upload.wikimedia.org/wikipedia/commons/c/c3/Python-logo-notext.svg">
  
This repository was published to share a simple method to verify, based on some measurements, how a group of binary categorical features are related each other. Particulary, this routine allows to answer the question: **_is there an association between a set of features and a main feature (response variable)?_**.\
The use case of this implementation is the capability to create fraud rules, for mitigation or prevention, that eventually migrate to the **SPSS Modeler routines** how control the customers use of digital channels.\
The procedure's foundations are easy to understand, details can be found [**here**](https://en.wikipedia.org/wiki/Association_rule_learning).

This time the way to consume the Spark resources is through the Python API: [**PySpark**](http://spark.apache.org/docs/2.2.0/api/python/index.html). The reason why is the ease of the Python language to write the program, as a _class_, that perform calculation over data represented on **pandas dataframes** using **lambda expresions**. The calculations inside the _class_ involves the **combinatorial operator** which is called from a Python module (_itertools_).

The measurements, early mentioned, are: **support** and **confidence**. Their interpretation is related to the concepts of **probability and the conditional probability**.

The routine is divided in **4** parts:

### 1. Modules (Modules.py): :books:
Import the PySpark classes needed to handle the data via data frame structure: **pandas**; sql functions to explore results; tab completion; combinatory function and set off the loglevel while executions of the routine.

### 2. ARL (ARL.py): :earth_americas:
A **Python** _class_ who calculates the **_support_** and **_confidence_** measures that allows to analyze the association between the set of fraud business features and the fraud class variable. By reading all the code structure it's quite possible to get how calculations are performed. This _class_ is the building block to the next ones.

### 3. Parameters (Parameters.py): :floppy_disk:
The **parameters** function calls the data, stored in **HDFS** thanks to a **Impala** ETL, its _return_ statament is build on the **ARL** _class_ self instance.

### 4. Write results (Write_results.py): :pencil:
Transforming the **pandas** data frame into **pyspark_sql_dataframe** using **StructField** and **StructType** methods, this function writes the results in **HDFS**. Those can be check by Impala or Hive.

######  **_Considerations_**:
###### 1. The current way to access Spark is through **_SFTP_** connection. **MobaXterm** is an alternative to doing so. However, it has no support, indeed, it has IT restrictions.
###### 2. Source codes in this repository can not be executed inside the GitHub platform.
###### 3. Updates published here are for the good of the version control. The new versions themselves don't migrate directly to the Landing Zone. The user has to copy these new versions into the node using, e.g., WinSPC or FileZilla.
