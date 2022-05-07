def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    selected = dfc.select(list(dfc.keys())[0]).toDF()
    
    from pyspark.sql.functions import regexp_replace as regxx
    
    newDF = selected.withColumn('ca', regxx('ca', '0', '6'))
    
    newDF = newDF.withColumn('thal', regxx('thal', '0', '8'))
    
    results = DynamicFrame.fromDF(newDF, glueContext, "results")
    return DynamicFrameCollection({"results": results}, glueContext)
