package cn.spark.study.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * @author Administrator
  */
object DataFrameCreate {

    def main(args: Array[String]) {
        val conf = new SparkConf()
                .setAppName("DataFrameCreate")
                .setMaster("local[*]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

//        val df = sqlContext.read.json("hdfs://spark1:9000/students.json")
        val df = sqlContext.read.json("data/sql/students.json")

        df.show()
    }

}