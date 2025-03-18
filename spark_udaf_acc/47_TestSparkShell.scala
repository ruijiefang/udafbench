//https://raw.githubusercontent.com/ashokblend/allprojects/5f48197276bc1011e9d6e2cfeb56d1fbe665ea68/SparkScala/src/main/scala/org/apache/spark/examples/TestSparkShell.scala
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Row
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

object TestSparkShell {
  
  def main(args:Array[String]){
    //mapPartition()
    //mapPartitionWithIndex
    //coalasce
    //bucket
    //saveasparquet
    //readparquet // run this after running saveasparquet
    //pairrdd
    //acc
    //stream
    //rdd
    //matrixmul(args)
    //phoenix_hbase
    //csv
    //orc_compare
    //hbase_binary
    //good_orc
    //corrupt_orc
  }
 
  def good_orc {
     val conf = new SparkConf().setMaster("local").set("spark.sql.orc.impl", "native").set("orc.skip.corrupt.data", "true")
    var spark = SparkSession.builder().appName("ORC")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    val tstdf = spark.read.format("orc").load("/Users/ashok.kumar/cases/nifi/00224554_avro2orc/out/") 
    tstdf.createOrReplaceTempView("nifi") 
    val maxsebnbr = spark.sql("select * from nifi") 
    //val maxsebnbr = spark.sql("select pat_seq_nbr from sattest3")
    //val maxsebnbr = spark.sql("describe formatted sattest3")
    maxsebnbr.show(1000) 
  }
  def corrupt_orc {
    val conf = new SparkConf().setMaster("local").set("spark.sql.orc.impl", "native").set("orc.skip.corrupt.data", "true")
    var spark = SparkSession.builder().appName("ORC")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    val tstdf = spark.read.format("orc").load("/Users/ashok.kumar/cases/spark/00220951_corrupt_orc/orc") 
    tstdf.createOrReplaceTempView("sattest3") 
    val maxsebnbr = spark.sql("select avg(pat_seq_nbr) from sattest3") 
    //val maxsebnbr = spark.sql("select pat_seq_nbr from sattest3")
    //val maxsebnbr = spark.sql("describe formatted sattest3")
    maxsebnbr.show(1000) 
  }
  def orc_compare {
      val conf = new SparkConf().setMaster("local")
      .set("spark.driver.extraClassPath", "/Users/ashok.kumar/junk/jarstore/phoenix/phoenix-spark2.jar")
      .set("spark.driver.extraJavaOptions","-Duser.timezone=UTC")
    var spark = SparkSession.builder().appName("ORC")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
  }
  def csv {
    val appName = "TestTimeZone"
    val conf = new SparkConf().setMaster("local")
      .set("spark.driver.extraClassPath", "/Users/ashok.kumar/junk/jarstore/phoenix/phoenix-spark2.jar")
      .set("spark.driver.extraJavaOptions","-Duser.timezone=UTC")
      .setJars(Array("/Users/ashok.kumar/github/spark/examples/lib/phoenix-spark2.jar","/Users/ashok.kumar/github/spark/examples/lib/phoenix-client.jar"))

    val phoption: scala.collection.immutable.HashMap[String, String] = scala.collection.immutable.HashMap(("zkUrl", "c2164-node4.squadron-labs.com:2181:/hbase-unsecure;c2164-node2.squadron-labs.com:/hbase-unsecure:2181;c2164-node3.squadron-labs.com:2181:/hbase-unsecure"), ("table", "datetest"))
    
    var spark = SparkSession.builder().appName(appName)
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
      val df =spark.sqlContext.load("org.apache.phoenix.spark",Map("table" -> "datetest", "zkUrl" -> "c2164-node4.squadron-labs.com:2181:/hbase-unsecure;c2164-node2.squadron-labs.com:/hbase-unsecure:2181;c2164-node3.squadron-labs.com:2181:/hbase-unsecure"))
      //val df = spark.read.format("org.apache.phoenix.spark").options(phoption).load()
      df.persist(StorageLevel.MEMORY_AND_DISK)
     val res = df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "false").option("delimiter","\u0001").option("timeZone", "UTC")

     res.save("/tmp/datetest.csv")

  }
  def hbase_binary() {
    val appName = "hbase_binary"
    val conf = new SparkConf().setMaster("local")
      .setJars(Array("/Users/ashok.kumar/github/spark/examples/lib/phoenix-client.jar"))

    val phoption: scala.collection.immutable.HashMap[String, String] = 
      scala.collection.immutable.HashMap(("zkUrl", "c3164-node4.squadron-labs.com:2181:/hbase-unsecure;c3164-node2.squadron-labs.com:/hbase-unsecure:2181;c3164-node3.squadron-labs.com:2181:/hbase-unsecure"),
          ("table", "binary"))
     
    var spark = SparkSession.builder().appName(appName)
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
     //spark.sql("select * from emp").show
    // System.exit(0)
    val df = spark.read.format("org.apache.phoenix.spark").options(phoption).load()
    df.persist()
    import org.apache.spark.sql.functions.udf
    val toString = udf((payload: Array[Byte]) => new String(payload))
    val convertedBinary = df.withColumn("convertedbinary", toString(df("bin")))
    convertedBinary.show()
    df.unpersist()
    //df.select("id","fname","lname",toString(df("bin"))).show
    //df.show()
    println("hi")
  }
  def phoenix_hbase() {
    val appName = "TestHbase"
    val conf = new SparkConf().setMaster("yarn")
      .setJars(Array("/Users/ashok.kumar/github/spark/examples/lib/phoenix-spark2.jar"))

    val columns = Array[String]("BI", "II", "CC", "SC", "INM", "UPP", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10", "F11", "F12", "F13", "F14", "F15", "F16", "F17", "F18", "F19", "F20", "F21", "F22", "F23", "F24", "F25", "F26", "F27", "F28", "F29", "F30", "F31", "F32", "F33", "F34", "F35", "F36", "F37", "F38", "F39", "F40", "F41", "F42", "F43", "F44", "F45", "F46", "F47", "F48", "F49", "F50", "F51", "F52", "F53", "F54", "F55", "F56", "F57", "F58", "F59", "F60", "F61", "F62", "F63", "F64", "F65", "F66", "F67", "F68", "F69", "F70", "F71", "F72", "F73", "F74", "F75", "F76", "F77", "F78", "F79", "F80", "F81", "F82", "F83", "F84", "F85", "F86", "F87", "F88", "F89", "F90", "F91", "F92", "F93", "F94", "F95", "F96", "F97", "F98", "F99", "V1", "V2", "V3", "V4", "V5", "ST", "GII", "PII", "PIST", "OMCD", "LST", "AD", "UD")

    val newnames = Array[String]("ITEMID", "BUSINESSID", "SUBCATEGORYN", "ITEMNUMBER", "UNITSPERPACKAGE", "FLD01", "FLD02", "FLD03", "FLD04", "FLD05", "FLD06", "FLD07", "FLD08", "FLD09", "FLD10", "FLD11", "FLD12", "FLD13", "FLD14", "FLD15", "FLD16", "FLD17", "FLD18", "FLD19", "FLD20", "FLD21", "FLD22", "FLD23", "FLD24", "FLD25", "FLD26", "FLD27", "FLD28", "FLD29", "FLD30", "FLD31", "FLD32", "FLD33", "FLD34", "FLD35", "FLD36", "FLD37", "FLD38", "FLD39", "FLD40", "FLD41", "FLD42", "FLD43", "FLD44", "FLD45", "FLD46", "FLD47", "FLD48", "FLD49", "FLD50", "FLD51", "FLD52", "FLD53", "FLD54", "FLD55", "FLD56", "FLD57", "FLD58", "FLD59", "FLD60", "FLD61", "FLD62", "FLD63", "FLD64", "FLD65", "FLD66", "FLD67", "FLD68", "FLD69", "FLD70", "FLD71", "FLD72", "FLD73", "FLD74", "FLD75", "FLD76", "FLD77", "FLD78", "FLD79", "FLD80", "FLD81", "FLD82", "FLD83", "FLD84", "FLD85", "FLD86", "FLD87", "FLD88", "FLD89", "FLD90", "FLD91", "FLD92", "FLD93", "FLD94", "FLD95", "FLD96", "FLD97", "FLD98", "FLD99", "STATUS", "ADDED", "UPDATED", "VFLD01", "VFLD02", "VFLD03", "VFLD04", "VFLD05", "COUNTRY_CODE", "GROUPITEMID", "PARENTITEMID", "PARENTITEMID_STATUS", "OUTLETITEM_MAP_CHANGE_DATE", "LOCKDOWN_STATUS")
    val phoption: scala.collection.immutable.HashMap[String, String] = scala.collection.immutable.HashMap(("zkUrl", "c2164-node4.squadron-labs.com:2181:/hbase-unsecure;c2164-node2.squadron-labs.com:/hbase-unsecure:2181;c2164-node3.squadron-labs.com:2181:/hbase-unsecure"), ("table", "positems"))
    var spark = SparkSession.builder().appName(appName)
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
     //spark.sql("select * from emp").show
    // System.exit(0)
      val df = spark.read.format("org.apache.phoenix.spark").options(phoption).load().where(col("BI") === "138").selectExpr(columns: _*)
    df.filter("SC is null").show()
    val positems = df.toDF(newnames: _*)
    positems.filter("itemid==138").show(20)

    val positems_cds = positems.withColumn("pk_closeout_data_state", lit("ADDED"))
    val positems_bi = positems_cds.withColumn("pk_business_id", lit("ITEMID"))
    positems_bi.filter("itemid==138").show(20)
    positems_bi.write.mode("append") insertInto ("ods_positems")

    df.show()
    println("hi")
  }
  def rdd {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val df1 = Seq(1,2,3,4,5).toDF("Key")
    val keyRdd = df1.rdd
    val df2 = Seq("test1","test2","test3","test4","test5").toDF("value")
    val valRdd = df2.rdd
    val zippedRdd = keyRdd.zipPartitions(valRdd){(keyIter, valIter) =>
      val keyList = keyIter.toSeq
      val valList = valIter.toSeq
      val data = keyList.zip(valList)
      data.iterator
    }
    zippedRdd.collect().foreach{ 
      case(key,value) => 
        println(key.getInt(0) + value.getString(0))
    }
     
  }
  def stream {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val schema = new StructType()
    schema.add("name",StringType)
    schema.add("city",StringType)
    val lines = spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe","spark")
                .schema(schema)
                .load()
                .selectExpr("Cast(name as String)","Cast(city as String)")
                //.as[String]
                //.flatMap(_.split(" "))
    lines.printSchema()
    //case class Record(name: String, city: String)
    
    //val a = lines.flatMap(_.split(" "))
    //val rec = lines.flatMap(_.split(" ")).toDF()
    //val query = rec.writeStream.format("parquet").option("checkpointLocation", "D:\\test").option("path", "D:\\test\\pq").start()
    //spark.sql("create table pqt(name string) using parquet options(path 'D:\\test\\pq')")
    //spark.sql("select * from pqt").show()
    //query.awaitTermination()
  }
  def acc {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val acc1: LongAccumulator=spark.sparkContext.longAccumulator("0")
    val acc2=spark.sparkContext.longAccumulator("0")
    val df =Seq(("a", 1, 1),("b", 2, 2),("c", 3, 3)).toDF("key","val1","val2")
    
    def myfilter(value: Int, acc:LongAccumulator): Boolean = {
      if (value < 2) {
        true
      } else {
        acc.add(1)
        false
      }
    }
    def myfilter1(value: Int): Boolean = myfilter(value,acc1)
    def myfilter2(value: Int): Boolean = myfilter(value,acc2)
   // val my_udf1 = udf(myfilter1, Boolean)
   // val my_udf2 = udf(myfilter2, BooleanType())
  }
  def pairrdd {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    //operation on employee.csv
    case class Person(name: String, mobile: String, mobileColor: String, cost: Int)
    val file = spark.sparkContext
                     .textFile("D:\\github\\FeatureTest\\input\\join\\mob.csv", 3)
                     .map{f: String =>
                             val line= f.split(",") 
                            (line(0) -> Person(line(0), line(1), line(2), line(3).toInt))
                         }
                     .reduceByKey((p1, p2) => {
                       Person(p1.name, p1.mobile, p1.mobileColor, (p1.cost + p2.cost))
                     })
                     .collect()
                     .foreach{
                       case (name: String, person: Person) =>println(name+":"+person.cost)
                     }
   
    val cdf = spark.read.csv("D:\\github\\FeatureTest\\input\\join\\mob.csv").toDF("name","mobile","mobilecolor","cost")
    val rdf = cdf.select("name", "cost").reduce((row1, row2)=>{
      (row1, row2) match {
        case (Row(name1: String, cost1: String),
              Row(name2: String, cost2: String)) if(name1.equals(name2)) =>
                Row(name1,  cost1.toInt+ cost2.toInt)   
        case _ => row1
      }
    })
    rdf match {
      case Row(name: String, cost: Int) => {
        println("name:"+name+",cost":+cost)
      }
    }
    System.exit(1)
    
    val words = Array("one", "two", "two", "three", "three", "three")
    val prdd = spark.sparkContext.parallelize(words, 3).map((_,1))
    //reducebykey
    def reducebykey(x: Int, y: Int):Int = {
      x+y
    }
    //prdd.reduceByKey(reducebykey).collect //one way
    prdd.reduceByKey(_+_).collect() //other way
    type B = (String, Int)
    //prdd.reduce((b1:B,b2:B)=>(b1._1,b1._2+b2._2))
    //groupbykey
    //prdd.groupByKey().collect()
    
  }
  
  def readparquet {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val df = spark.read.format("parquet").load("D:\\data.parquet")
    .toDF()
    df.show()
    
  }
  def saveasparquet {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val data = (0 to 1000)
                .map(i=>(i,s"data_$i"))
                .toDF("key","value")
                .write
                .format("parquet")
                .save("D:\\data.parquet")
                
  }
  def bucket {
   val spark = SparkSession.builder().master("local").getOrCreate()
   import spark.implicits._
   
   val data = (0 to 1000000)
                 .map(i => (i, s"master_$i"))
                 .toDF("key","value")
                 .write
                 .format("json")
                 .bucketBy(3, "key")
                 .mode(SaveMode.Overwrite)
                 .saveAsTable("master_json")
                 
   //
   val transaction = (0 to 1000000)
                     .map(i => (i, s"transaction_$i"))
                     .toDF("key", "value")
                     .repartition(3, 'key)
                     .cache()

   transaction.count()
   

   //
   spark.sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "0")

   val master = spark
                  .read
              .table("master_json")

   val results = master.join(transaction, "key")
   results.explain(true)
   results.collect()
   spark.stop()
                 
  }
  def coalasce() {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val fileRdd=spark.sparkContext.textFile("D:\\github\\FeatureTest\\input\\join\\employee.csv",3)
    println("coalasce testing by increasing no of partition")
    val coalsRdd = fileRdd.coalesce(15, true)
    import spark.sqlContext.implicits._
    //converting to dataframe
    println(coalsRdd.toDF().explain())
    var res =coalsRdd.mapPartitionsWithIndex{
      (index,iterator) => {
        iterator.toList.map{x=>
          println(index+"->"+x)
          x.split(",")(0) -> index
        }.iterator
      }
    }
    val cr =res.collect()
    cr.length
  }
  def mapPartitionWithIndex() {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val fileRdd=spark.sparkContext.textFile("D:\\github\\FeatureTest\\input\\join\\employee.csv",3)
    //map partition with index
    println("map partition with index")
    val mapPartIndxRes= fileRdd.mapPartitionsWithIndex{
      (index,iterator) => {
         iterator.toList.map{x:String =>
           (x.split(",")(0),index)
         }.toIterator
      }
    }
    println(mapPartIndxRes.collect().mkString(":"))
    
  }
  def mapPartition(){
    val spark = SparkSession.builder().master("local").getOrCreate()
    val fileRdd=spark.sparkContext.textFile("D:\\github\\FeatureTest\\input\\join\\employee.csv",3)
    //map partition
    println("map partition")
    val mapPartRes = fileRdd.mapPartitions{
      iterator => {
        iterator.toList.map{x =>
          x.split(",")(0)
        }.iterator
      }
    }
    println(mapPartRes.collect().mkString(","))
    
  }
  
  def matrixmul(args:Array[String]) {
    
      //here file1 and file2 will have two matrix to be multiplied
      // matrix should be in below form
      // row, colum, value
      // e.g 0 , 0, 10 => represents 0th row , 0th column value is 10
      val file1=args(0)
      val file2=args(1)
      val fields=Array(StructField("rindx",IntegerType,false),
          StructField("cindx",IntegerType,true),
          StructField("value",IntegerType,true))
      val spark=SparkSession.builder().master("local").getOrCreate()
      val schema=StructType(fields)
          
      val df1 = spark.sqlContext.read.format("csv").option("header", false).schema(schema).load(file1)
      val df2= spark.sqlContext.read.format("csv").option("header", false).schema(schema).load(file2)
      
      val colCount= df1.agg(max(df1.col("cindx"))).collect()(0).get(0)
      val rowCount= df2.agg(max(df2.col("rindx"))).collect()(0).get(0)
      
      //compare dimension for feasibility of mulitiplication
      require(colCount==rowCount,s"Dimension mismatch: $colCount,$rowCount")
      //grouping each row data for first rdd
      val rd1=df1.rdd.map(row => {
        val key:Int = row.getInt(0)
        val value:List[Int]=List(row.getInt(2))
        key -> value
      })
      def reduceop(t1:List[Int], t2:List[Int]):List[Int]={
        val a=t1.++(t2)
        a
      }
      val rowData = rd1.reduceByKey(reduceop)
      
      //grouping each column data for second rdd
      val rd2=df2.rdd.map(row => {
        val key:Int = row.getInt(1)
        val value:List[Int]=List((row.getInt(2)))
        key -> value
      })
      val colData = rd2.reduceByKey(reduceop)
      
      val res1 = rowData.collect()
      val res2 = colData.collect()
      
      //Note this, join, will give us only digonal element of matrix
      //val diag = rowData.join(colData).collect
      
      val cart = rowData.cartesian(colData).groupByKey()
      val resultMatrix = cart.map(row =>{
        val rowId = row._1._1
        val rowData = row._1._2
        val colsData = row._2
        val result=ListBuffer[Int]()
        val mulData = colsData.foreach(x =>{
          val colId= x._1
          val colData = x._2
          var r:Int=0;
          for(i <- 0 until colData.length) {
            r=r+colData(i)*rowData(i)
          }
          result +=r
        })
        rowId -> result
      })
      println("Result of matrix multiplication:")
      val result = resultMatrix.sortByKey(true, 3).collect()
      result.foreach(row => {
        row._2.foreach(value =>{
          print(value +"  ")
        })
        println
        
      })
    
  }
}