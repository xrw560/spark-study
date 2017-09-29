package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
  * @author Administrator
  */
object WindowHotWord {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WindowHotWord")
        val ssc = new StreamingContext(conf, Seconds(1))

        val searchLogsDStream = ssc.socketTextStream("spark1", 9999)
        val searchWordsDStream = searchLogsDStream.map {
            _.split(" ")(1)
        }
        val searchWordPairsDStream = searchWordsDStream.map { searchWord => (searchWord, 1) }
        val searchWordCountsDStream = searchWordPairsDStream.reduceByKeyAndWindow(
            (v1: Int, v2: Int) => v1 + v2,
            Seconds(60),
            Seconds(10))

        val finalDStream = searchWordCountsDStream.transform(searchWordCountsRDD => {
            // 反转
            val countSearchWordsRDD = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
            // 排序
            val sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false)
            // 反转回来
            val sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.map(tuple => (tuple._1, tuple._2))

            val top3SearchWordCounts = sortedSearchWordCountsRDD.take(3)
            for (tuple <- top3SearchWordCounts) {
                println(tuple)
            }

            searchWordCountsRDD
        })

        // 这个无关紧要，只是为了触发job的执行，所以必须有output操作
        finalDStream.print()

        ssc.start()
        ssc.awaitTermination()
    }

}