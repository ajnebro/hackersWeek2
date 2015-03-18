package es.uma.hackersweek.sparkdemo;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Example implementing the Spark version of the "Hello World" Big Data program: counting the
 * number of occurrences of words in a set of files
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class SparkDemoCountWordsFromFiles {
  static Logger log = Logger.getLogger(SparkDemoCountWordsFromFiles.class.getName());

  public static void main(String[] args) {

    // STEP 1: create a SparkConf object
    if (args.length < 1) {
      log.fatal("Syntax Error: there must be one argument (a file name or a directory)")  ;
      throw new RuntimeException();
    }

    // STEP 2: create a SparkConf object
    SparkConf sparkConf = new SparkConf().setAppName("Spark Word count") ;

    // STEP 3: create a Java Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf) ;

    // STEP 4: read lines of files
    JavaRDD<String> lines = sparkContext.textFile(args[0]);

    // STEP 5: split the lines into words
    JavaRDD<String> words = lines.flatMap(
        new FlatMapFunction<String, String>() {
          public Iterable call(String s) throws Exception {
            return Arrays.asList(s.split(" "));
          }
        }) ;

    // STEP 6: map operation to create pairs <word, 1> por each word
    JavaPairRDD<String, Integer> ones = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          public Tuple2<String, Integer> call(String string) {
            return new Tuple2<String, Integer>(string, 1);
          }
        }
    ) ;

    // STEP 6: reduce operation that sum the values of all the pairs having the same key (word),
    //         generating a pair <key, sum>
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          public Integer call(Integer integer, Integer integer2) throws Exception {
            return integer + integer2 ;
          }
        }
    ) ;

    // STEP 7: sort the results by key
    List<Tuple2<String, Integer>> output = counts.sortByKey().collect() ;

    // STEP 8: print the results
    for (Tuple2<?, ?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2()) ;
    }

    // STEP 9: stop the spark context
    sparkContext.stop();
  }
}
