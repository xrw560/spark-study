package cn.ibeifeng.spark

import org.apache.spark.sql.SparkSession

/**
 * untyped操作：观察一下就会发现，实际上基本上就涵盖了普通sql语法的全部了
 * 
 * sql语法
 * 		select
 * 		where
 * 		join
 * 		groupBy
 * 		agg
 * 
 */
object UntypedOperation {
  
  def main(args: Array[String]) {
    // 来改造一下之前讲解的那个统计部门平均薪资和年龄的案例
    // 将所有的untyped算子都应用进去
    
    val spark = SparkSession
        .builder()
        .appName("UntypedOperation") 
        .master("local") 
        .config("spark.sql.warehouse.dir", "C:\\Users\\Administrator\\Desktop\\spark-warehouse")
        .getOrCreate()
    
    import spark.implicits._
    import org.apache.spark.sql.functions._
    
    val employee = spark.read.json("C:\\Users\\Administrator\\Desktop\\employee.json")
    val department = spark.read.json("C:\\Users\\Administrator\\Desktop\\department.json")
    
    employee
        .where("age > 20")
        .join(department, $"depId" === $"id") 
        .groupBy(department("name"), employee("gender"))
        .agg(avg(employee("salary")))
        .show() 
        
     employee
         .select($"name", $"depId", $"salary")
         .where("age > 30")
         .show()  
  }
  
}