package de.hpi.fgis.dbda.textmining.entrance_task;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

/**
 * Hello world!
 *
 */
public class Main 
{
    public static void main( String[] args )
    {
    	final MaxentTagger tagger = new MaxentTagger("pos_tagger/taggers/english-bidirectional-distsim.tagger");
    	
    	// String tagged = tagger.tagString(sample);
        
        // initialize spark environment
        SparkConf config = new SparkConf().setAppName(Main.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");
        
        try(JavaSparkContext context = new JavaSparkContext(config)) {
        	 JavaRDD<String> lineItems = context
        	    .textFile("data/nyt.1987.tsv")
	        	.map(
	            new Function<String, String>() {
	              public String call(String line) {
	                LineItem li = new LineItem(line);
	                return li.TEXT;
	              }
	            });
	            
	            lineItems.saveAsTextFile("data/test");
        }
    }
    
    static class LineItem implements Serializable {
        Integer INDEX;
        String TEXT;

        LineItem(String line) {
          String[] values = line.split("\t");
          INDEX = Integer.parseInt(values[0]);
          TEXT = values[1];
        }
      }
}
