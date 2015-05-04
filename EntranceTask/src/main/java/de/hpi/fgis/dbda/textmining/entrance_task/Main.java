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
            //generate Pairs of <<word,tag>,1>
            JavaPairRDD<Tuple2<String, String>, Integer> pairs = wordTags
            		.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {

                        @Override
                        public Tuple2<Tuple2<String, String>, Integer> call(String arg0)
                                throws Exception {
                            String[] splitted = arg0.split("_");
                            Tuple2 inner = new Tuple2<>(splitted[0], splitted[1]);
                            Tuple2 outer = new Tuple2<>(inner, 1);
                            return outer;
                        }
                    });

            //##################################
            //most common tag per word
            //##################################

            //sum counts <<word, tag>, n>
            JavaPairRDD<Tuple2<String, String>, Integer> counts = pairs
                    .reduceByKey(new Function2<Integer, Integer, Integer>() {

                        @Override
                        public Integer call(Integer arg0, Integer arg1)
                                throws Exception {
                            return arg0 + arg1;
                        }
                    });

            //transform to pairs <word, <tag, n>>
            JavaPairRDD<String, Tuple2<String, Integer>> transformedCounts = counts
                    .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> t) throws Exception {
                            Tuple2 inner = new Tuple2<>(t._1._2, t._2);
                            return new Tuple2(t._1._1, inner);
                        }
                    });

            //reduce to top tags per word
            JavaPairRDD<String, Tuple2<String, Integer>> topTags = transformedCounts
                    .reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> call(Tuple2<String, Integer> tag1, Tuple2<String, Integer> tag2) throws Exception {
                            if (tag1._2 > tag2._2) {
                                return tag1;
                            } else {
                                return tag2;
                            }
                        }
                    });

            transformedCounts.sortByKey().saveAsTextFile("data/besttag");

            //##################################
            //most different tags per word
            //##################################

            //distinct <word, tag>, 1>
            JavaPairRDD<Tuple2<String, String>, Integer> distinctPairs = pairs.distinct();

            //transform to pairs <word, 1>
            JavaPairRDD<String, Integer> foo = distinctPairs
                    .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {
                        @Override
                        public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Integer> t) throws Exception {
                            return new Tuple2<String, Integer>(t._1._1, 1);
                        }
                    });

            //summed up <word, n>
            JavaPairRDD<String, Integer> counted = foo
                    .reduceByKey(new Function2<Integer, Integer, Integer>() {
                        @Override
                        public Integer call(Integer integer1, Integer integer2) throws Exception {
                            return integer1 + integer2;
                        }
                    });

            JavaRDD<Tuple2<String, Integer>> tagsBeforeSort = counted.map(new Function<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    return new Tuple2<String, Integer>(stringIntegerTuple2._1, stringIntegerTuple2._2);
                }
            });
            
            tagsBeforeSort.saveAsTextFile("data/mosttags");
            
            List<Tuple2<String, Integer>> sorted = tagsBeforeSort.
            	takeOrdered(10, new TagComp());

            System.out.println(sorted);
        }
    }

    static class WordComp implements Comparator<Tuple3<String, String, Integer>>, Serializable {

		@Override
		public int compare(Tuple3<String, String, Integer> o1,
				Tuple3<String, String, Integer> o2) {
			return Integer.compare(o2._3(), o1._3());
		}

      }

    static class TagComp implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> o1,
                           Tuple2<String, Integer> o2) {
            return Integer.compare(o2._2(), o1._2());
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