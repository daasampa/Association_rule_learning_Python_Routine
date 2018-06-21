def table_result(segment, num_features):
    support = ARL.support_calculation(parameters(segment, num_features))
    fields_support = [StructField(field_name, StringType(), True)
                      for field_name
                      in ['support'] +
                         [''.join(x) for x in zip(np.repeat('feature_', support.shape[1] - 1),
                                                  map(lambda x: str(x), range(support.shape[1] - 1)))]]
    schema_support = StructType(fields_support)
    support_DF = spark.createDataFrame(support, schema_support)
    support_DF.createOrReplaceTempView("support_calculation_"+ segment.replace(" ", "") + "_n" +
                                        str(num_features))
    spark.sql("drop table if exists proceso_seguridad_externa." + "support_calculation_" +
              segment.replace(" ", "") + "_n" + str(num_features))
    spark.sql("create table if not exists proceso_seguridad_externa." + "support_calculation_" +
              segment.replace(" ", "") + "_n" + str(num_features) +
              " as select * from support_calculation_" + segment.replace(" ", "") + "_n" +
              str(num_features))
    confidence = ARL.confidence_calculation(parameters(segment, num_features), support)
    fields_confidence = [StructField(field_name, StringType(), True)
                         for field_name
                         in ['confidence'] +
                         [''.join(x) for x in zip(np.repeat('feature_', confidence.shape[1] - 1),
                                                  map(lambda x: str(x), range(confidence.shape[1] - 1)))]]
    schema_confidence = StructType(fields_confidence)
    confidence_DF = spark.createDataFrame(confidence, schema_confidence)
    confidence_DF.createOrReplaceTempView("confidence_calculation_"+ segment.replace(" ", "") + "_n" +
                                           str(num_features))
    spark.sql("drop table if exists proceso_seguridad_externa." + "confidence_calculation_" +
              segment.replace(" ", "") + "_n" + str(num_features))
    spark.sql("create table if not exists proceso_seguridad_externa." + "confidence_calculation_" +
              segment.replace(" ", "") + "_n" + str(num_features) +
              " as select * from confidence_calculation_" + segment.replace(" ", "") + "_n" +
              str(num_features))    