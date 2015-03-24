package es.uma.hackersweek.sparkdemo;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Example implementing the Spark version of the "Hello World" Big Data program: counting the
 * number of occurrences of words in a set of files
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class SparkDemoCountWordsFromFilesCompactVersion {
	static Logger log = Logger.getLogger(SparkDemoCountWordsFromFilesCompactVersion.class.getName());

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

		// STEP 6: reduce operation that sum the values of all the pairs having the same key (word),
		//         generating a pair <key, sum>
		JavaPairRDD<String, Integer> counts = lines.flatMap(s -> Arrays.asList(s.split(" ")))
				.mapToPair(string -> new Tuple2<String, Integer>(string, 1))
				.reduceByKey((integer, integer2) -> integer + integer2) ;

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
