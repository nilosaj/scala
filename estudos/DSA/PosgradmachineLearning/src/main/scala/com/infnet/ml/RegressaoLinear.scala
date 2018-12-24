
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.{col, lower, round}

def main(): Unit = {
    import org.apache.log4j._

    Logger.getLogger("org").setLevel(Level.ERROR)

    import org.apache.spark.sql.SparkSession

    import org.apache.spark.sql.functions.{concat, lit, when}

    val sparks = SparkSession.builder().master("local").getOrCreate()

    import sparks.implicits._

    println("Iniciando criação de dataframes")
  
    val filePath = "file:///Users/nilosaj/Desenvolvimento/projetos/datasets/posgrad_ml/"

    val obt2013 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"Obitos/Obitos-2013.csv")
      .withColumn("ano",lit("2013"))
      .select(lower($"municipios").as("municipios"), $"obitos",$"ano")

    val obt2012 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"Obitos/Obitos-2012.csv")
      .withColumn("ano",lit("2012"))
      .select(lower($"municipios").as("municipios"), $"obitos",$"ano")

    val obt2011 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"Obitos/Obitos-2011.csv")
      .withColumn("ano",lit("2011"))
      .select(lower($"municipios").as("municipios"), $"obitos",$"ano")

    val obt2010 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"Obitos/Obitos-2010.csv")
      .withColumn("ano",lit("2010"))
      .select(lower($"municipios").as("municipios"), $"obitos",$"ano")

    println("Dataframes anuais de óbitos criados")

    val leitos2013 = sparks.read.option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";")
      .csv(filePath+"leitos/Leitos-2013.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .withColumn("ano",lit("2013"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS",$"ano")

    val leitos2012 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"leitos/Leitos-2012.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .withColumn("ano",lit("2012"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS",$"ano")

    val leitos2011 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"leitos/Leitos-2011.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .withColumn("ano",lit("2011"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS",$"ano")

    val leitos2010 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"leitos/Leitos-2010.csv")
      .withColumn("NSUS", when($"NSUS".isNull, 0).otherwise($"NSUS"))
      .withColumn("ano",lit("2010"))
      .select(lower($"municipios").as("municipios"), $"qexistente", $"SUS", $"NSUS",$"ano")


  println("Dataframes anuais de leitos criados")


    val pibc2013 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"pibcapta/PIB_CAPTA_2013.csv")
      .withColumn("ano",lit("2013"))
      .select(lower($"municipios").as("municipios"), $"pibcapta",$"ano")

    val pibc2012 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"pibcapta/PIB_CAPTA_2012.csv")
      .withColumn("ano",lit("2012"))
      .select(lower($"municipios").as("municipios"), $"pibcapta",$"ano")

    val pibc2011 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"pibcapta/PIB_CAPTA_2011.csv")
      .withColumn("ano",lit("2011"))
      .select(lower($"municipios").as("municipios"), $"pibcapta",$"ano")

    val pibc2010 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"pibcapta/PIB_CAPTA_2010.csv")
      .withColumn("ano",lit("2010"))
      .select(lower($"municipios").as("municipios"), $"pibcapta",$"ano")

  println("Dataframes anuais de PIBs criados")

    val evitaveis2013 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"evitaveis/EVITAVEIS-2013.csv")
      .withColumn("ano",lit("2013"))
      .withColumn("rd_imunoprev", when($"rd_imunoprev".isNull, 0).otherwise($"rd_imunoprev"))
      .withColumn("rd_infectoprev", when($"rd_infectoprev".isNull, 0).otherwise($"rd_infectoprev"))
      .withColumn("rd_naotransmissprev", when($"rd_naotransmissprev".isNull, 0).otherwise($"rd_naotransmissprev"))
      .withColumn("rd_causamaterna", when($"rd_causamaterna".isNull, 0).otherwise($"rd_causamaterna"))
      .withColumn("rd_causaexterna", when($"rd_causaexterna".isNull, 0).otherwise($"rd_causaexterna"))
      .withColumn("mal_definidas", when($"mal_definidas".isNull, 0).otherwise($"mal_definidas"))
      .withColumn("nao_evitaveis", when($"nao_evitaveis".isNull, 0).otherwise($"nao_evitaveis"))
      .withColumn("Total", when($"Total".isNull, 0).otherwise($"Total"))
      .select(lower($"municipios").as("municipios"),$"rd_imunoprev",$"rd_infectoprev",$"rd_naotransmissprev",$"rd_causamaterna",$"rd_causaexterna",$"mal_definidas",$"nao_evitaveis",$"Total",$"ano")

    val evitaveis2012 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"evitaveis/EVITAVEIS-2012.csv")
      .withColumn("ano",lit("2012"))
      .withColumn("rd_imunoprev", when($"rd_imunoprev".isNull, 0).otherwise($"rd_imunoprev"))
      .withColumn("rd_infectoprev", when($"rd_infectoprev".isNull, 0).otherwise($"rd_infectoprev"))
      .withColumn("rd_naotransmissprev", when($"rd_naotransmissprev".isNull, 0).otherwise($"rd_naotransmissprev"))
      .withColumn("rd_causamaterna", when($"rd_causamaterna".isNull, 0).otherwise($"rd_causamaterna"))
      .withColumn("rd_causaexterna", when($"rd_causaexterna".isNull, 0).otherwise($"rd_causaexterna"))
      .withColumn("mal_definidas", when($"mal_definidas".isNull, 0).otherwise($"mal_definidas"))
      .withColumn("nao_evitaveis", when($"nao_evitaveis".isNull, 0).otherwise($"nao_evitaveis"))
      .withColumn("Total", when($"Total".isNull, 0).otherwise($"Total"))
      .select(lower($"municipios").as("municipios"),$"rd_imunoprev",$"rd_infectoprev",$"rd_naotransmissprev",$"rd_causamaterna",$"rd_causaexterna",$"mal_definidas",$"nao_evitaveis",$"Total",$"ano")


    val evitaveis2011 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"evitaveis/EVITAVEIS-2011.csv")
      .withColumn("ano",lit("2011"))
      .withColumn("rd_imunoprev", when($"rd_imunoprev".isNull, 0).otherwise($"rd_imunoprev"))
      .withColumn("rd_infectoprev", when($"rd_infectoprev".isNull, 0).otherwise($"rd_infectoprev"))
      .withColumn("rd_naotransmissprev", when($"rd_naotransmissprev".isNull, 0).otherwise($"rd_naotransmissprev"))
      .withColumn("rd_causamaterna", when($"rd_causamaterna".isNull, 0).otherwise($"rd_causamaterna"))
      .withColumn("rd_causaexterna", when($"rd_causaexterna".isNull, 0).otherwise($"rd_causaexterna"))
      .withColumn("mal_definidas", when($"mal_definidas".isNull, 0).otherwise($"mal_definidas"))
      .withColumn("nao_evitaveis", when($"nao_evitaveis".isNull, 0).otherwise($"nao_evitaveis"))
      .withColumn("Total", when($"Total".isNull, 0).otherwise($"Total"))
      .select(lower($"municipios").as("municipios"),$"rd_imunoprev",$"rd_infectoprev",$"rd_naotransmissprev",$"rd_causamaterna",$"rd_causaexterna",$"mal_definidas",$"nao_evitaveis",$"Total",$"ano")


    val evitaveis2010 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"evitaveis/EVITAVEIS-2010.csv")
      .withColumn("ano",lit("2010"))
      .withColumn("rd_imunoprev", when($"rd_imunoprev".isNull, 0).otherwise($"rd_imunoprev"))
      .withColumn("rd_infectoprev", when($"rd_infectoprev".isNull, 0).otherwise($"rd_infectoprev"))
      .withColumn("rd_naotransmissprev", when($"rd_naotransmissprev".isNull, 0).otherwise($"rd_naotransmissprev"))
      .withColumn("rd_causamaterna", when($"rd_causamaterna".isNull, 0).otherwise($"rd_causamaterna"))
      .withColumn("rd_causaexterna", when($"rd_causaexterna".isNull, 0).otherwise($"rd_causaexterna"))
      .withColumn("mal_definidas", when($"mal_definidas".isNull, 0).otherwise($"mal_definidas"))
      .withColumn("nao_evitaveis", when($"nao_evitaveis".isNull, 0).otherwise($"nao_evitaveis"))
      .withColumn("Total", when($"Total".isNull, 0).otherwise($"Total"))
      .select(lower($"municipios").as("municipios"),$"rd_imunoprev",$"rd_infectoprev",$"rd_naotransmissprev",$"rd_causamaterna",$"rd_causaexterna",$"mal_definidas",$"nao_evitaveis",$"Total",$"ano")

  println("Dataframes anuais de causas evitáveis criados")

    val equipamentos2013 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"equipamentos/EQUIPAMENTOS-2013.csv")
      .withColumn("ano",lit("2013"))
      .withColumn("audiologia", when($"audiologia".isNull, 0).otherwise($"audiologia"))
      .withColumn("diagnistico_img", when($"diagnistico_img".isNull, 0).otherwise($"diagnistico_img"))
      .withColumn("infra_estrutura", when($"infra_estrutura".isNull, 0).otherwise($"infra_estrutura"))
      .withColumn("odontologia", when($"odontologia".isNull, 0).otherwise($"odontologia"))
      .withColumn("manutencao_vida", when($"manutencao_vida".isNull, 0).otherwise($"manutencao_vida"))
      .withColumn("metodos_graficos", when($"metodos_graficos".isNull, 0).otherwise($"metodos_graficos"))
      .withColumn("metodos_opticos", when($"metodos_opticos".isNull, 0).otherwise($"metodos_opticos"))
      .withColumn("outros", when($"outros".isNull, 0).otherwise($"outros"))
      .withColumn("Total", when($"Total".isNull, 0).otherwise($"Total"))
      .select(lower($"municipios").as("municipios"),$"audiologia",$"diagnistico_img",$"infra_estrutura",$"odontologia",$"manutencao_vida",$"metodos_graficos",$"metodos_opticos",$"outros",$"Total".as("total_equips"),$"ano")



    val equipamentos2012 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"equipamentos/EQUIPAMENTOS-2012.csv")
      .withColumn("ano",lit("2012"))
      .withColumn("audiologia", when($"audiologia".isNull, 0).otherwise($"audiologia"))
      .withColumn("diagnistico_img", when($"diagnistico_img".isNull, 0).otherwise($"diagnistico_img"))
      .withColumn("infra_estrutura", when($"infra_estrutura".isNull, 0).otherwise($"infra_estrutura"))
      .withColumn("odontologia", when($"odontologia".isNull, 0).otherwise($"odontologia"))
      .withColumn("manutencao_vida", when($"manutencao_vida".isNull, 0).otherwise($"manutencao_vida"))
      .withColumn("metodos_graficos", when($"metodos_graficos".isNull, 0).otherwise($"metodos_graficos"))
      .withColumn("metodos_opticos", when($"metodos_opticos".isNull, 0).otherwise($"metodos_opticos"))
      .withColumn("outros", when($"outros".isNull, 0).otherwise($"outros"))
      .withColumn("Total", when($"Total".isNull, 0).otherwise($"Total"))
      .select(lower($"municipios").as("municipios"),$"audiologia",$"diagnistico_img",$"infra_estrutura",$"odontologia",$"manutencao_vida",$"metodos_graficos",$"metodos_opticos",$"outros",$"Total".as("total_equips"),$"ano")



    val equipamentos2011 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"equipamentos/EQUIPAMENTOS-2011.csv")
      .withColumn("ano",lit("2011"))
      .withColumn("audiologia", when($"audiologia".isNull, 0).otherwise($"audiologia"))
      .withColumn("diagnistico_img", when($"diagnistico_img".isNull, 0).otherwise($"diagnistico_img"))
      .withColumn("infra_estrutura", when($"infra_estrutura".isNull, 0).otherwise($"infra_estrutura"))
      .withColumn("odontologia", when($"odontologia".isNull, 0).otherwise($"odontologia"))
      .withColumn("manutencao_vida", when($"manutencao_vida".isNull, 0).otherwise($"manutencao_vida"))
      .withColumn("metodos_graficos", when($"metodos_graficos".isNull, 0).otherwise($"metodos_graficos"))
      .withColumn("metodos_opticos", when($"metodos_opticos".isNull, 0).otherwise($"metodos_opticos"))
      .withColumn("outros", when($"outros".isNull, 0).otherwise($"outros"))
      .withColumn("Total", when($"Total".isNull, 0).otherwise($"Total"))
      .select(lower($"municipios").as("municipios"),$"audiologia",$"diagnistico_img",$"infra_estrutura",$"odontologia",$"manutencao_vida",$"metodos_graficos",$"metodos_opticos",$"outros",$"Total".as("total_equips"),$"ano")


    val equipamentos2010 = sparks.read.option("header","true")
      .option("inferSchema","true")
      .option("delimiter",";")
      .csv(filePath+"equipamentos/EQUIPAMENTOS-2010.csv")
      .withColumn("ano",lit("2010"))
      .withColumn("audiologia",lit("0"))
      .withColumn("diagnistico_img", when($"diagnistico_img".isNull, 0).otherwise($"diagnistico_img"))
      .withColumn("infra_estrutura", when($"infra_estrutura".isNull, 0).otherwise($"infra_estrutura"))
      .withColumn("odontologia", when($"odontologia".isNull, 0).otherwise($"odontologia"))
      .withColumn("manutencao_vida", when($"manutencao_vida".isNull, 0).otherwise($"manutencao_vida"))
      .withColumn("metodos_graficos", when($"metodos_graficos".isNull, 0).otherwise($"metodos_graficos"))
      .withColumn("metodos_opticos", when($"metodos_opticos".isNull, 0).otherwise($"metodos_opticos"))
      .withColumn("outros", when($"outros".isNull, 0).otherwise($"outros"))
      .withColumn("Total", when($"Total".isNull, 0).otherwise($"Total"))
      .select(lower($"municipios").as("municipios"),$"audiologia",$"diagnistico_img",$"infra_estrutura",$"odontologia",$"manutencao_vida",$"metodos_graficos",$"metodos_opticos",$"outros",$"Total".as("total_equips"),$"ano")

  println("Dataframes anuais de equipamentos criados")

    val popul2013 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"populacao/POPULACAO_EST-2013.csv")
      .withColumn("ano",lit("2013"))
      .select(lower($"municipios").as("municipios"), $"populacao",$"ano")


    val popul2012 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"populacao/POPULACAO_EST-2012.csv")
      .withColumn("ano",lit("2012"))
      .select(lower($"municipios").as("municipios"), $"populacao",$"ano")


    val popul2011 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"populacao/POPULACAO_EST-2011.csv")
      .withColumn("ano",lit("2011"))
      .select(lower($"municipios").as("municipios"), $"populacao",$"ano")


    val popul2010 = sparks.read.option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(filePath+"populacao/POPULACAO_EST-2010.csv")
      .withColumn("ano",lit("2010"))
      .select(lower($"municipios").as("municipios"), $"populacao",$"ano")

  println("Dataframes anuais de população estimada criados")

  println("Efetuando Joins de Dataframes de óbitos")

    val dfObitos = obt2013
      .union(obt2012)
      .union(obt2011)
      .union(obt2010)

  println("Efetuando Joins de Dataframes de leitos")

    val dfLeitos =  leitos2013
      .union(leitos2012)
      .union(leitos2011)
      .union(leitos2010)

  println("Efetuando Joins de Dataframes de PIBs")

    val dfPib = pibc2013
      .union(pibc2012)
      .union(pibc2011)
      .union(pibc2010)

  println("Efetuando Joins de Dataframes de causas evitaveis")

    val dfEvitaveis = evitaveis2013
      .union(evitaveis2012)
      .union(evitaveis2011)
      .union(evitaveis2010)

  println("Efetuando Joins de Dataframes de equipamentos")

    val dfEquip = equipamentos2013
      .union(equipamentos2012)
      .union(equipamentos2011)
      .union(equipamentos2010)

  println("Efetuando Joins de Dataframes de população estimada")

    val dfPopulacao = popul2013
      .union(popul2012)
      .union(popul2011)
      .union(popul2010)

    val proporcaoSUS= List(col("SUS"),col("qexistente"))
    val proporcaoImunoPop= List(col("populacao"),col("rd_imunoprev"))
    val proporcaoInfectoPop= List(col("populacao"),col("rd_infectoprev"))
    val proporcaoCausExterna= List(col("populacao"),col("rd_causaexterna"))
    val proporcaoMalDef= List(col("populacao"),col("mal_definidas"))


  println("Efetuando Joins de Dataframes consolidados e  adição de colunas de proporção (COLUNAS EXISTENTES / POPULACAO)")

    val pessoasPorLeito=List(col("populacao"),col("qexistente"))

    val dfCompleteAll = dfObitos
      .join(dfLeitos, Seq(("municipios"),("ano")))
      .distinct()
      .join(dfPib, Seq(("municipios"),("ano")))
      .distinct()
      .join(dfEvitaveis, Seq(("municipios"),("ano")))
      .distinct()
      .join(dfEquip, Seq(("municipios"),("ano")))
      .distinct()
      .join(dfPopulacao, Seq(("municipios"),("ano")))
      .distinct()
      .withColumn("pessoas_por_leito",round(pessoasPorLeito.reduce( _/_ )))
      .withColumn("proporcao_sus",proporcaoSUS.reduce( _/_ ))
      .withColumn("proporcao_imuno",proporcaoImunoPop.reduce( _/_ ))
      .withColumn("proporcao_infecto",proporcaoInfectoPop.reduce( _/_ ))
      .withColumn("proporcao_externa",proporcaoCausExterna.reduce( _/_ ))
      .withColumn("proporcao_mal_definid",proporcaoMalDef.reduce( _/_ ))
      .withColumn("proporcao_imuno", when($"proporcao_imuno".isNull, 0).otherwise($"proporcao_imuno"))
      .withColumn("proporcao_infecto", when($"proporcao_infecto".isNull, 0).otherwise($"proporcao_infecto"))
      .withColumn("proporcao_externa", when($"proporcao_externa".isNull, 0).otherwise($"proporcao_externa"))
      .withColumn("proporcao_mal_definid", when($"proporcao_mal_definid".isNull, 0).otherwise($"proporcao_mal_definid"))

    dfCompleteAll.show(5, false)

    dfCompleteAll.printSchema()

    //println("Alguns Filtros")
    //dfCompleteAll.filter(dfCompleteAll("obitos") > 60000).select($"obitos",$"pessoas_por_leito").show(10)

    val finalDf = dfCompleteAll.select($"obitos".as("label"), $"municipios", $"rd_infectoprev", $"rd_naotransmissprev",$"rd_causaexterna", $"nao_evitaveis",$"pessoas_por_leito")

    val Array(treino,teste) = finalDf.randomSplit(Array(0.7,0.3), seed= 1111)

    import org.apache.spark.ml.regression.LinearRegression
    import org.apache.spark.ml.feature.VectorAssembler

    println("Iniciando Vector Assembler")

    val va = new VectorAssembler().setInputCols(Array("rd_infectoprev", "rd_naotransmissprev", "rd_causaexterna","nao_evitaveis")).setOutputCol("features").setHandleInvalid("skip")


    val output = va.transform(treino).select($"label", $"features")

    output.show(20)

    val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)



    println("Criando modelo para colunas rd_infectoprev , rd_naotransmissprev , rd_causaexterna , nao_evitaveis")
    val model = lr.fit(output)

    println("Executando dados de teste")

    val testOutput = va.transform(teste).select($"label", $"features")
    model.transform(testOutput).show()
    model.coefficients.toArray.foreach(X => println("Coeficiente: "+X.toString() ) )
    println(s"Intercept :  ${model.intercept} ")
    println("")
    val summary = model.summary

    println("Resultados finais:")
    println(s"numero de iterações: ${summary.totalIterations}")
    println("Residuos")
    summary.residuals.show()

    println(s"RMSE: ${summary.rootMeanSquaredError}")
    println(s"MSE: ${summary.meanSquaredError}")
    println(s"R2 (Coeficiente de Determinação) : ${summary.r2}")   //coeficiente de ajuste


    println("Predições")
    summary.predictions.show()
  }


main()