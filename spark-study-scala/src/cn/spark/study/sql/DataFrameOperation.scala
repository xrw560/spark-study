package cn.spark.study.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * @author Administrator
  */
object DataFrameOperation {

    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf()
                .setAppName("DataFrameCreate").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

//        val df = sqlContext.read.json("hdfs://spark1:9000/students.json")
        val df = sqlContext.read.json("data/sql/students.json")

        df.show()
        df.printSchema()
        df.select("name").show()
        df.select(df("name"), df("age") + 1).show()
        df.filter(df("age") > 18).show()
        df.groupBy("age").count().show()
    }

}