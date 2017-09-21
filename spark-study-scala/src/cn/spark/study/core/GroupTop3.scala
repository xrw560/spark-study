package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._

/**
  * Created by Administrator on 2017/9/21.
  */
object GroupTop3 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setAppName("Top3")
          .setMaster("local")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("data/score.txt", 1)
        val pairs = lines.map(x => {
            val splited = x.split(" ")
            (splited(0), splited(1).toInt)
        })

        val groupedPairs = pairs.groupByKey()

        val top3Score = groupedPairs.map(classScore => {
            val top3 = Array[Int](-1, -1, -1)

            val className = classScore._1

            val scores = classScore._2

            for (score <- scores) {
                breakable {
                    for (i <- 0 until 3) {
                        if (top3(i) == -1) {
                            top3(i) = score
                            break
                        } else if (score > top3(i)) {
                            var j = 2
                            while (j > i) {
                                top3(j) = top3(j - 1)
                                j = j - 1
                            }
                            top3(i) = score
                            break
                        }
                    }
                }
            }
            (className, top3)
        })

        top3Score.foreach(x => {
            println(x._1)
            val res = x._2
            for (i <- res) {
                println(i)
            }
            println("==============================")
        })

    }

}
