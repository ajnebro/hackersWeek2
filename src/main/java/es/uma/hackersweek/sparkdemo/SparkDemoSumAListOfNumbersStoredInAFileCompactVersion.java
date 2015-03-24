package es.uma.hackersweek.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Example showing how to read data from files. The applications sum a list of integer values.
 * Each data file must contain an integer per line. Compact version using lambda expression 
 * and fluent code.
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class SparkDemoSumAListOfNumbersStoredInAFileCompactVersion {
	public static void main(String[] args) {

		// STEP 1: argument checking
		if (args.length == 0) {
			throw new RuntimeException("The number of args is 0. Usage: "
					+ "SparkDemoSumAListOfNumbersStoredInAFileCompactVersion fileOrDirectoryName") ;
		}

		// STEP 2: create a SparkConf object
		SparkConf conf = new SparkConf().setAppName("SparkDemo") ;

		// STEP 3: create a Java Spark context
		JavaSparkContext sparkContext = new JavaSparkContext(conf) ;

		// STEP 4: read the lines from the file(s)
		JavaRDD<String> lines = sparkContext.textFile(args[0]) ;

		// STEP 5: compute the sum
		int sum = lines.map(s -> Integer.valueOf(s)).reduce((integer, integer2) -> integer+integer2) ;

		// STEP 6: print the result
		System.out.println("The sum is: " + sum) ;

		// STEP 8: stop de spark context
		sparkContext.stop();  
	}
}
