package cn.ibeifeng.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

/**
 * 初步入门SparkSession、Dataframe、Dataset的基本开发
 */
object SparkSQLDemo {
  
  // 定义一个case class
  // 会用dataset，通常都会通过case class来定义dataset的数据结构，自定义类型其实就是一种强类型，也就是typed
  case class Person(name: String, age: Long)
  // 定义操作hive数据的case class
  case class Record(key: Int, value: String)
  
  def main(args: Array[String]) {
    // 构造SparkSession，基于build()来构造
    val spark = SparkSession
        .builder()  // 用到了java里面的构造器设计模式
        .appName("SparkSQLDemo") 
//        .master("local")   
        // spark.sql.warehouse.dir，必须设置
        // 这是Spark SQL 2.0里面一个重要的变化，需要设置spark sql的元数据仓库的目录
//        .config("spark.sql.warehouse.dir", "C:\\Users\\Administrator\\Desktop\\spark-warehouse")  
        // 启用hive支持
        .enableHiveSupport()
        .getOrCreate()
    
    import spark.implicits._
    
    // 读取json文件，构造一个untyped弱类型的dataframe
    // dataframe就相当于Dataset[Row]
    val df = spark.read.json("hdfs://spark2upgrade01:9000/test_data/people.json")
    df.show()  // 打印数据
    df.printSchema()  // 打印元数据
    df.select("name").show()  // select操作，典型的弱类型，untyped操作
    df.select($"name", $"age" + 1).show()  // 使用表达式，scala的语法，要用$符号作为前缀
    df.filter($"age" > 21).show()  // filter操作+表达式的一个应用
    df.groupBy("age").count().show()  // groupBy分组，再聚合
    
    // 基于dataframe创建临时视图
    df.createOrReplaceTempView("people")
    // 使用SparkSession的sql()函数就可以执行sql语句，默认是针对创建的临时视图的
    val sqlDF = spark.sql("SELECT * FROM people")  
    sqlDF.show()  
    
    // 直接基于jvm object来构造dataset
    val caseClassDS = Seq(Person("Andy", 32)).toDS() 
    caseClassDS.show()
    // 基于原始数据类型构造dataset
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).show()
    // 基于已有的结构化数据文件，构造dataset
    // spark.read.json()，首先获取到的是dataframe，其次使用as[Person]之后，就可以将一个dataframe转换为一个dataset
    val peopleDS = spark.read.json("hdfs://spark2upgrade01:9000/test_data/people.json").as[Person]  
    peopleDS.show()  
    
    // 创建hive表
    spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH '/usr/local/test_data/kv1.txt' INTO TABLE src")
    spark.sql("SELECT * FROM src").show()
    spark.sql("SELECT COUNT(*) FROM src").show()
    
    val sqlHiveDF = spark.sql("SELECT key,value FROM src WHERE key < 10 ORDER BY key")  
    val sqlHiveDS = sqlHiveDF.map {
        case Row(key: Int, value: String) => s"KEY: $key, VALUE: $value"    
    }
    sqlHiveDS.show()  
    
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")  
    spark.sql("SELECT * FROM src JOIN records ON src.key=records.key").show()  
  }
  
}