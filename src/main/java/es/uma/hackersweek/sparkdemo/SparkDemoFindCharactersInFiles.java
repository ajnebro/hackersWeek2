package es.uma.hackersweek.sparkdemo;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Spark applications which counts the number of lines of a file (or the files in a directory)
 * having an 'a' an a 'b'
 *
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 *
 *
 */
public class SparkDemoFindCharactersInFiles {
  static Logger log = Logger.getLogger(SparkDemoFindCharactersInFiles.class.getName());

  public static void main(String[] args) throws ClassNotFoundException {

    // STEP 1: argument checking
    if (args.length < 1) {
      log.fatal("Syntax Error: there must be one argument (a file or directory name)")  ;
      throw new RuntimeException();
    }

    String fileName = args[0];

    // STEP 2: create a SparkConf object
    SparkConf sparkConf = new SparkConf().setAppName("FindCharacters");

    // STEP 3: create a Java Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    // STEP 4: read lines of files
    JavaRDD<String> lines = sparkContext.textFile(fileName).cache();

    // STEP 5: filter and count the number of lines containing the character 'a'
    long numAs = lines.filter(new Function<String, Boolean>() {
      public Boolean call(String s) {
        return s.contains("a");
      }
    }).count();

    // STEP 6: filter and count the number of lines containing the character 'b'
    long numBs = lines.filter(new Function<String, Boolean>() {
      public Boolean call(String s) {
        return s.contains("b");
      }
    }).count();

    // STEP 8: print the results
    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    // STEP 9: stop the spark context
    sparkContext.stop();
  }
}
