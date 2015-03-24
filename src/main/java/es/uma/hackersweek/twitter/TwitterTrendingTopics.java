package es.uma.hackersweek.twitter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Example showing how to count the most used words in files containing tweets
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class TwitterTrendingTopics {
  public static void main(String[] args) {

    // STEP 1: argument checking
    if (args.length == 0) {
      throw new RuntimeException("The number of args is 0. Usage: "
          + "TwitterTrendingTopics fileOrDirectoryName") ;
    }

    // STEP 2: create a SparkConf object
    SparkConf conf = new SparkConf().setAppName("TwitterTrendingTopics") ;

    // STEP 3: create a Java Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(conf) ;

    // STEP 4: read the lines from the file(s)
    JavaRDD<String> lines = sparkContext.textFile(args[0]) ;

    // STEP 5: map to select the strings in the column 2, which contains the tweet message
    JavaRDD<String> messages = lines.map(new Function<String, String>() {
      public String call(String s) {
        String [] split = s.toString().split("\t+") ;

        return split[2];
      }
    });

    // STEP 5: split the lines into words
    JavaRDD<String> words = messages.flatMap(
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

    // STEP 7: invert the keys and values to use sortByKey() in step 8 on the values instead of th
    //         keys
    JavaPairRDD<Integer, String> reverse = counts.mapToPair(
        new PairFunction<Tuple2<String, Integer>, Integer, String>() {
      @Override public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2)
          throws Exception {
        return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
      }
    }) ;

    /*// STEP 8: sort the results by key
    List<Tuple2<Integer, String>> output = reverse.sortByKey().collect() ;

    // STEP 9.1: print the results to screen
    for (Tuple2<?, ?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2()) ;
    }*/

    // STEP 9.2 print the results to file
    reverse.sortByKey().saveAsTextFile("data/tweets/output");

    // STEP 10: stop de spark context
    sparkContext.stop();  }
}
