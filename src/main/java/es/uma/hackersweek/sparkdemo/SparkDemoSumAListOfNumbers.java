package es.uma.hackersweek.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Example to show how to start a Spark application and create and work with a JavaRDD (Resilient
 * Distributed Data Set).
 *
 * The application sum a list of integer values.
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class SparkDemoSumAListOfNumbers {
  public static void main(String[] args) {
    // STEP 1: create a SparkConf object
    SparkConf conf = new SparkConf().setAppName("SparkDemo") ;

    // STEP 2: create a Java Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(conf) ;

    // An array of Integer objects
    Integer[] values = new Integer[]{1, 2, 3, 4, 5, 6, 8, 9} ;

    // A list of Integers
    List<Integer> data = Arrays.asList(values);

    // STEP 3: create a JavaRDD
    JavaRDD<Integer> distributedData = sparkContext.parallelize(data);

    // STEP 4: compute the sum
    int sum = distributedData.reduce(new Function2<Integer, Integer, Integer>() {
      @Override public Integer call(Integer integer, Integer integer2) throws Exception {
        return integer+integer2;
      }
    }) ;

    // STEP 5: print the result
    System.out.println("The sum is: " + sum) ;

    // STEP 6: stop de spark context
    sparkContext.stop();
  }
}
