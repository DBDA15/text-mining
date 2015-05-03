package de.hpi.fgis.dbda.textmining.entrance_task;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

public class Main
{
	static transient MaxentTagger tagger = null;

    public static void main( String[] args )
    {

        // String tagged = tagger.tagString(sample);

        // initialize spark environment
        SparkConf config = new SparkConf().setAppName(Main.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
            JavaRDD<String> lineItems = context
                    .textFile("data/short.tsv");
            //tag articles
            JavaRDD<String> taggedLines = lineItems
                    .map(
                            new Function<String, String>() {
                                public String call(String line) {
                                    if (tagger == null) {
                                        tagger = new MaxentTagger("pos_tagger/taggers/english-left3words-distsim.tagger");
                                    }
                                    LineItem li = new LineItem(line);
                                    String tagged = tagger.tagString(li.TEXT);
                                    //System.out.println(tagged);
                                    return tagged;
                                }
                            });
            //split tagged articles into words
            JavaRDD<String> wordTags = taggedLines
                    .flatMap(
                            new FlatMapFunction<String, String>() {
                                @Override
                                public Iterable<String> call(String tagLine) throws Exception {
                                    String[] splitTagLine = tagLine.split(" ");
                                    List<String> splitTagLineList = Arrays.asList(splitTagLine);
                                    return splitTagLineList;
                                }
                            }
                    );
            //generate Pairs of <word, <tag, 1>>
            JavaPairRDD<Tuple2<String, String>, Integer> pairs = wordTags
            		.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {

						@Override
						public Tuple2<Tuple2<String, String>, Integer> call(String arg0)
								throws Exception {
							String[] splitted = arg0.split("_");
							Tuple2 inner = new Tuple2<String, String>(splitted[0], splitted[1]);
							Tuple2 outer = new Tuple2<Tuple2, Integer>(inner, 1);
							return outer;
						}
					});
            
            JavaPairRDD<Tuple2<String, String>, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

				@Override
				public Integer call(Integer arg0, Integer arg1)
						throws Exception {					
					return arg0+arg1;
				}
			});
            
            JavaPairRDD<Tuple3<String, String, Integer>, Void> beforeSort = counts.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>,Integer>, Tuple3<String, String, Integer>, Void>() {

				@Override
				public Tuple2<Tuple3<String, String, Integer>, Void> call(
						Tuple2<Tuple2<String, String>, Integer> arg0) throws Exception {
					Tuple3<String, String, Integer> key = new Tuple3<String, String, Integer>(arg0._1._1, arg0._1._2, arg0._2);
					return new Tuple2<Tuple3<String,String,Integer>,Void>(key, null);
				}
			});
			
            JavaPairRDD<Tuple3<String, String, Integer>, Void> sorted = beforeSort.sortByKey(new WordComp());
                        
            // map -> split "_" -> Tuple ( Tuple (word, tag), count=1)
    		// reduceByKey (Tuple(word,tag))
    		// sort by count
    		// tag per word: distinct word
    		// word per tag: distinct tag
    		// different tags per word: 
            
            sorted.saveAsTextFile("data/test");
        }
    }

    static class WordComp implements Comparator<Tuple3<String, String, Integer>>, Serializable {

		@Override
		public int compare(Tuple3<String, String, Integer> o1,
				Tuple3<String, String, Integer> o2) {
			return Integer.compare(o2._3(), o1._3());
		}

      }

    static class LineItem implements Serializable {
        Integer INDEX;
        String TEXT;

        LineItem(String line) {
            String[] values = line.split("\t");
            INDEX = Integer.parseInt(values[0]);
            TEXT = values[4];
        }
    }
}