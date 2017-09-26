package cn.spark.study.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作
 *
 * @author Administrator
 */
public class GenericLoadSave {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf()
                           .setAppName("GenericLoadSave");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame usersDF = sqlContext.read().load("hdfs://spark1:9000/users.parquet");
        usersDF.printSchema();
        usersDF.show();
        usersDF.select("name", "favorite_color").write()
          .save("hdfs://spark1:9000/namesAndFavColors.parquet");
    }

}
