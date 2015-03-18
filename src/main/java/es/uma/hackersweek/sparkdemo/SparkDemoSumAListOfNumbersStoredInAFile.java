package es.uma.hackersweek.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Example showing how to read data from files. The applications sum a list of integer values.
 * Each data file must contain an integer per line.
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class SparkDemoSumAListOfNumbersStoredInAFile {
  public static void main(String[] args) {

    // STEP 1: argument checking
    if (args.length == 0) {
      throw new RuntimeException("The number of args is 0. Usage: "
          + "SparkDemoSumAListOfNumberStoredInAFile fileOrDirectoryName") ;
    }

    // STEP 2: create a SparkConf object
    SparkConf conf = new SparkConf().setAppName("SparkDemo") ;

    // STEP 3: create a Java Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(conf) ;

    // STEP 4: read the lines from the file(s)
    JavaRDD<String> lines = sparkContext.textFile(args[0]) ;

    // STEP 5: map the string coded numbers into integers
    JavaRDD<Integer> listOfNumbers = lines.map(new Function<String, Integer>() {
      @Override public Integer call(String s) throws Exception {
        return Integer.valueOf(s);
      }
    }) ;

    // STEP 6: compute the sum
    int sum = listOfNumbers.reduce(new Function2<Integer, Integer, Integer>() {
      @Override public Integer call(Integer integer, Integer integer2) throws Exception {
        return integer+integer2;
      }
    }) ;

    // STEP 7: print the result
    System.out.println("The sum is: " + sum) ;

    // STEP 8: stop de spark context
    sparkContext.stop();  }
}
