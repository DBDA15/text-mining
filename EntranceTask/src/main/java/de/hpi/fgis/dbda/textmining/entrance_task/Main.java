package de.hpi.fgis.dbda.textmining.entrance_task;

import java.io.Serializable;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * Hello world!
 *
 */
public class Main 
{
    public static void main( String[] args )
    {
    	// String tagged = tagger.tagString(sample);

        // initialize spark environment
        SparkConf config = new SparkConf().setAppName(Main.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
        	 JavaRDD<String> lineItems = context
        	    .textFile("data/nyt.1987.tsv")
	        	.mapPartitionsWithIndex(
                        new Function2<Integer, Iterator<String>, Iterator<String>>() {
                            public Iterator<String> call(Integer i, Iterator<String> iter) {
                                final MaxentTagger tagger = new MaxentTagger("pos_tagger/taggers/english-bidirectional-distsim.tagger");
                                List res = new ArrayList();
                                while (iter.hasNext()) {
                                    String str = iter.next();
                                    LineItem li = new LineItem(str);
                                    String tagged = tagger.tagString(li.TEXT);
                                    res.add(tagged);
                                }
                                return res.iterator();
                            }
                        }, true);
	            
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
