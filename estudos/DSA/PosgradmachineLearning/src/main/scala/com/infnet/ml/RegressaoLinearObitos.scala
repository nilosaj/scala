package com.infnet.ml

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.sql.functions.{lower,col,round}

object RegressaoLinearObitos {

  def main(args: Array[String]): Unit = {
    import org.apache.log4j._

    Logger.getLogger("org").setLevel(Level.ERROR)

    import org.apache.spark.sql.SparkSession

    import org.apache.spark.sql.functions.{concat, lit, when}

    val sparks = SparkSession.builder().master("local").getOrCreate()

    import sparks.implicits._

    val obt2016 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos/Obitos-2016.csv")
      .select(lower($"municipios").as("municipios"), $"obitos")

    val obt2015 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos/Obitos-2015.csv")
      .select(lower($"municipios").as("municipios"), $"obitos")

    val obt2014 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos/Obitos-2014.csv")
      .select(lower($"municipios").as("municipios"), $"obitos")

    val obt2013 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos/Obitos-2013.csv")
      .select(lower($"municipios").as("municipios"), $"obitos")

    val obt2012 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos/Obitos-2012.csv")
      .select(lower($"municipios").as("municipios"), $"obitos")

    val obt2011 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos/Obitos-2011.csv")
      .select(lower($"municipios").as("municipios"), $"obitos")

    val obt2010 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Obitos/Obitos-2010.csv")
      .select(lower($"municipios").as("municipios"), $"obitos")


    val leitos2016 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Leitos/Leitos-2016.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS")

    val leitos2015 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Leitos/Leitos-2015.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS")

    val leitos2014 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Leitos/Leitos-2014.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS")

    val leitos2013 = sparks.read.option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/leitos/Leitos-2013.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS")

    val leitos2012 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/leitos/Leitos-2012.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS")

    val leitos2011 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/leitos/Leitos-2011.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS")

    val leitos2010 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/leitos/Leitos-2010.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS")


    val baseMunicipios = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .format("csv")
      .load("file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/Base_de_dados_dos_municipios.csv")
      .select(concat($"Codmun", lit(" "), lower($"NomeMunic")).as("municipios"), $"VAR01".as("populacao"))

    obt2016.show(5, false)
    leitos2016.show(5, false)
    baseMunicipios.show(5)


    val dfObitos = obt2016.union(obt2015)
    .union(obt2014)
    .union(obt2013)
    .union(obt2012)
    .union(obt2011)
    .union(obt2010)

    val dfLeitos = leitos2016.union(leitos2015)
    .union(leitos2014)
    .union(leitos2013)
    .union(leitos2012)
    .union(leitos2011)
    .union(leitos2010)
      .filter(col("qexistente") > 50)

    val pessoasPorLeito = List(col("populacao"),col("qexistente"))
    val proporcaoSUS= List(col("SUS"),col("qexistente"))

    val dfCompleteAll = dfObitos
              .join(dfLeitos, Seq("municipios"))
              .distinct()
              .join(baseMunicipios,Seq("municipios"))
              .distinct()
              .withColumn("pessoas_por_leito",round(pessoasPorLeito.reduce( _/_ )))
              .withColumn("proporcao_sus",proporcaoSUS.reduce( _/_ ))
              .filter(col("obitos") < 1500)
              .filter(col("proporcao_sus") > 0.8)
              .filter(col("pessoas_por_leito") < 2000)

    dfCompleteAll.show(20, false)


    val finalDf = dfCompleteAll.select($"obitos".as("label"), $"municipios", $"qexistente", $"SUS", $"NSUS",$"pessoas_por_leito")
    finalDf.show(5)

    import org.apache.spark.ml.regression.LinearRegression
    import org.apache.spark.ml.feature.VectorAssembler
    //import org.apache.spark.ml.tuning.{ParamGridBuilder,TrainValidationSplit}
    //import org.apache.spark.ml.linalg.Vectors


    val va = new VectorAssembler().setInputCols(Array("qexistente", "SUS", "NSUS","pessoas_por_leito")).setOutputCol("features").setHandleInvalid("skip")

    val output = va.transform(finalDf).select($"label", $"features")

    output.show(20)

    val lr = new LinearRegression()

    dfCompleteAll.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter", ";").save("completeCsv")

    val model = lr.fit(output)


    println(s"Coeficientes : ${model.coefficients}   Intercept :  ${model.intercept} ")

    val summTraining = model.summary

    println(s"numero de iterações: ${summTraining.totalIterations}")

    summTraining.residuals.show()
    summTraining.predictions.show()

    println(s"RMSE: ${summTraining.rootMeanSquaredError}")
    println(s"MSE: ${summTraining.meanSquaredError}")
    println(s"R2: ${summTraining.r2}")
  }

}
