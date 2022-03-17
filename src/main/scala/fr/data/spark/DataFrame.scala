
package fr.data.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object DataFrame {

  def loadData(): DataFrame = {

    val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("Spark CSV Reader")
        .getOrCreate;

    spark.read
         .format("csv")
         .option("header", "true") //first line in file has headers
         .option("mode", "DROPMALFORMED")
         .option("delimiter", ";")
         .load("./src/main/resources/codesPostaux.csv")
  }

  def displaySchema(df : DataFrame): Unit = {
    df.printSchema()
  }

  def countCommunes(df : DataFrame) : Long = {
    df.select("Nom_commune").count()
  }

  def counCommunesL5(df : DataFrame) : Unit ={
    println("Count des communes contennant la colonne Ligne_5 qui n'est pas nulle:")
    df.select("Nom_commune").filter("Ligne_5 is not null").show()
  }

  def addDepartmentNumber(df : DataFrame): DataFrame = {
    df.withColumn("DepNumber",col("Code_postal").substr(0, 2))
  }

  def createCsv(df : DataFrame): Unit = {
    df.write
    .mode("overwrite")
    .option("header",true)
    .option("delimiter", ";")
    .csv("./src/main/resources/commune_et_departement.csv")
  }

  def displayAisne(df: DataFrame): Unit = {
    df.select("Nom_commune").filter(df("DepNumber") === "02").show()
  }

  def getBiggerDep(df: DataFrame): Unit = {
    df("DepNumber").map(dep => (dep,1)).reduceByKey(_+_)
  }

  //Quel est le département avec le plus de communes ?
  def getBiggerDep(df: DataFrame): DataFrame = {
    df.select("departement", "Code_commune_INSEE")
      .distinct()
      .groupBy("departement")
      .count()
      .orderBy(desc("Count"))
  }

  def main(args: Array[String]): Unit = {
  
    val df = loadData()

    //Quel est le schéma du fichier ? 
    displaySchema(df)

    //Affichez le nombre de communes.
    df.show(5)

    //Affichez le nombre de communes qui possèdent l’attribut Ligne_5
    println("Nombre de communes : " + countCommunes(df))
    counCommunesL5(df)

    println("Df with dep number:")
    addDepartmentNumber(df).show()

    println("Création du CSV....")
    createCsv(addDepartmentNumber(df))
    println("CSV créé avec succès !")

    println("Affichage des commune de l'Aisne: ")
    displayAisne(addDepartmentNumber(df))

    println("Nombre de communes par département:")
    getBiggerDep(addDepartmentNumber(df))

    
   

    

  }


}
