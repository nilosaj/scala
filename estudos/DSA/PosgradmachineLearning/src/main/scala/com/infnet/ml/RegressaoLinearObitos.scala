package com.infnet.ml

object RegressaoLinearObitos {

  def main(args: Array[String]): Unit = {
    import org.apache.log4j._

    Logger.getLogger("org").setLevel(Level.ERROR)

    import org.apache.spark.sql.SparkSession

    import org.apache.spark.sql.functions.{concat,lit,when}

    val sparks = SparkSession.builder().master("local").getOrCreate()

    import sparks.implicits._

    val obt2016 = sparks.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos_causas_evitaveis_2013.csv")
    val obt2015 = sparks.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos_causas_evitaveis_2015.csv")
    val obt2014 = sparks.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos_causas_evitaveis_2014.csv")
    val obt2013 = sparks.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos_causas_evitaveis_2013.csv")

    val leitos2016 = sparks.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Leitos-Fim-2016.csv").withColumn("NSUS",when($"NSUS".isNull,0).otherwise($"NSUS"))
    val leitos2015 = sparks.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Leitos-Fim-2015.csv").withColumn("NSUS",when($"NSUS".isNull,0).otherwise($"NSUS"))
    val leitos2014 = sparks.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Leitos-Fim-2014.csv").withColumn("NSUS",when($"NSUS".isNull,0).otherwise($"NSUS"))
    val leitos2013 = sparks.read.option("header","true").option("inferSchema","true").option("delimiter",";").format("csv").load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Leitos-Fim-2013.csv").withColumn("NSUS",when($"NSUS".isNull,0).otherwise($"NSUS"))



    //obt2016.printSchema()
    val dfObt2016 = obt2016.select(concat($"microregiao",lit(" 2016")).as("o_microregiao_ano"),$"obitos")
    val dfObt2015 = obt2015.select(concat($"microregiao",lit(" 2015")).as("o_microregiao_ano"),$"obitos")
    val dfObt2014 = obt2014.select(concat($"microregiao",lit(" 2014")).as("o_microregiao_ano"),$"obitos")
    val dfObt2013 = obt2013.select(concat($"microregiao",lit(" 2013")).as("o_microregiao_ano"),$"obitos")


    val dfLeitos2016 = leitos2016.select(concat($"microregiao",lit(" 2016")).as("l_microregiao_ano"), $"Quantidade_existente" , $"SUS",$"NSUS")//.withColumn("NSUS",when($"NSUS".isNull,0).otherwise($"NSUS").otherwise($"NSUS"))
    val dfLeitos2015 = leitos2015.select(concat($"microregiao",lit(" 2015")).as("l_microregiao_ano"), $"Quantidade_existente" , $"SUS",$"NSUS")//.withColumn("NSUS",when($"NSUS".isNull,0).otherwise($"NSUS").otherwise($"NSUS"))
    val dfLeitos2014 = leitos2014.select(concat($"microregiao",lit(" 2014")).as("l_microregiao_ano"), $"Quantidade_existente" , $"SUS",$"NSUS")//.withColumn("NSUS",when($"NSUS".isNull,0).otherwise($"NSUS").otherwise($"NSUS"))
    val dfLeitos2013 = leitos2013.select(concat($"microregiao",lit(" 2013")).as("l_microregiao_ano"), $"Quantidade_existente" , $"SUS",$"NSUS")//.withColumn("NSUS",when($"NSUS".isNull,0).otherwise($"NSUS").otherwise($"NSUS"))





    val dfCompleteObt = dfObt2016.union(dfObt2015).union(dfObt2014).union(dfObt2013)
    val dfCompleteLeitos = dfLeitos2016.union(dfLeitos2015).union(dfLeitos2014).union(dfLeitos2013)//.withColumn("Quantidade_Nao_SUS",when($"Quantidade_Nao_SUS".isNull,0))

    val dfCompleteAll = dfCompleteObt.join(dfCompleteLeitos,dfCompleteObt("o_microregiao_ano") === dfCompleteLeitos("l_microregiao_ano"),"inner").drop("l_microregiao_ano")

    println("Complete Obitos")
    dfCompleteObt.show(5)



    println("Complete Leitos")
    println(dfCompleteLeitos.show(10))

    println("Complete Complete All : "+dfCompleteAll.count())
    //dfCompleteAll.printSchema()
    //dfCompleteAll.show(5)

    val finalDf = dfCompleteAll.select($"obitos".as("label"),$"o_microregiao_ano",$"Quantidade_existente",$"SUS",$"NSUS")
    finalDf.show(5)

    import org.apache.spark.ml.regression.LinearRegression
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.linalg.Vectors

    val va = new VectorAssembler().setInputCols(Array("Quantidade_existente","SUS","NSUS")).setOutputCol("features")

    var output = va.transform(finalDf).select($"label",$"features")

    output.show()

    val lr = new LinearRegression()
    val model = lr.fit(output)

    println(s"Coeficientes : ${model.coefficients}   Intercept :  ${model.intercept} ")
  }


}
