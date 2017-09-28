package cn.spark.study.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * @author Administrator
  */
object RDD2DataFrameProgrammatically extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
            .setMaster("local")
            .setAppName("RDD2DataFrameProgrammatically")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 第一步，构造出元素为Row的普通RDD
    val studentRDD = sc.textFile("data/sql/students.txt", 1)
            .map { line => Row(line.split(",")(0).toInt, line.split(",")(1), line.split(",")(2).toInt) }

    // 第二步，编程方式动态构造元数据
    val structType = StructType(Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)))

    // 第三步，进行RDD到DataFrame的转换
    val studentDF = sqlContext.createDataFrame(studentRDD, structType)

    // 继续正常使用
    studentDF.registerTempTable("students")

    // 大家可以这么理解，执行到这一行代码的时候
    // 调用了SQLContext.sql()方法，会用SqlParser生成一个Unresolved LogicalPlan，并封装在DataFrame中
    val teenagerDF = sqlContext.sql("select * from students where age<=18")

    // 后面去用这个DataFrame的时候，获取了其中的RDD，并执行了transformation和action操作
    // 那么就会触发SqlContext的executeSql()方法，该方法中，就会触发QueryExecution的执行
    // 这里第二步，调用Analyser的apply()方法，使用Unresolved LogicalPlan生成Resolved LogicalPlan
    // 其实最重要的，就是，将我们的LogicalPlan与SQL语句中的数据源绑定起来

    // 比如，这里Unresolved LogicalPlan中，只是针对select * from students where age<18这条SQL语句生成了
    // 一个树的结果，比如：
    // PROJECT name
    //      ||
    // SELECT
    // students
    //      ||
    // WHERE age<18
    // 但是，实际上，此时最关键的一点是，不知道students表，是哪个表，表在哪里？mysql?hive?临时表
    // 临时表，又在哪里？
    // 那么，Analyser的apply()方法调用后，生成的Resolved LogicalPlan，就与SQL语句中的数据源，students临时表
    // (studentDF.registerTempTable('students'))，进行绑定
    // 此时，Resolved LogicalPlan中，就知道了，自己要从哪个数据源中进行查询
    val teenagerRDD = teenagerDF.rdd.collect().foreach { row => println(row) }
}