from pyspark.sql.functions import *
from pyspark.sql.types import *
from itertools import combinations, compress
import pandas as pd, numpy as np, readline, inspect, time
readline.parse_and_bind("tab: complete")
spark.sparkContext.setLogLevel("OFF")
