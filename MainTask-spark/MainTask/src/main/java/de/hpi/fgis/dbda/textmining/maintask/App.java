package de.hpi.fgis.dbda.textmining.maintask;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;
import scala.Tuple5;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.util.CoreMap;

public class App
{

    private static transient AbstractSequenceClassifier<? extends CoreMap> classifier = null;

    private static String joinTuples(List<Tuple2> tupleList, String separator) {
        /*
          Join the first string elements of all 2-tuples in a given tuple list (like StringUtils.join())
        */
        List<String> strings = new ArrayList<>();
        for (Tuple2<String, String> tuple : tupleList) {
            strings.add(tuple._1());
        }
        return StringUtils.join(strings, separator);
    }

    private static Map produceContext(List<Tuple2> tokenList) {
        /*
           Produce the context based on a given token list. A context is a HashMap that maps each token in the token
           list to a weight. The more prominent or frequent a token is, the higher is the associated weight.
         */
        Map<String, Integer> termCounts = new HashedMap();

        //Count how often each token occurs
        Integer sumCounts = 0;
        for (Tuple2<String, String> token : tokenList) {
            if (!termCounts.containsKey(token._1())) {
                termCounts.put(token._1(), 1);
            } else {
                termCounts.put(token._1(), termCounts.get(token._1()) + 1);
            }
            sumCounts += 1;
        }

        //Calculate token frequencies out of the counts
        Map<String, Float> context = new HashedMap();
        for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
            context.put(entry.getKey(), (float) entry.getValue() / sumCounts);
        }
        return context;
    }

    public static void main( String[] args )
    {

        final String outputFile = args[0];

        //Initialize entity tags for the relation extraction
        final List<String> task_entityTags = new ArrayList<>();
        task_entityTags.add("ORGANIZATION");
        task_entityTags.add("LOCATION");

        //Initialize list of seed tuples
        final List<Tuple2> task_seedTuples = new ArrayList<>();
        task_seedTuples.add(new Tuple2("Microsoft", "Redmond"));
        task_seedTuples.add(new Tuple2("Google", "Palo Alto"));
        task_seedTuples.add(new Tuple2("Apple", "Cupertino"));
        task_seedTuples.add(new Tuple2("Exxon Corporation", "Irving"));

        //Initialize spark environment
        SparkConf config = new SparkConf().setAppName(App.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
        	
        	JavaRDD<String> lineItems = null;
        	
        	for (int i = 1; i < args.length; i++) {
        		if (lineItems == null) {
        			lineItems = context.textFile(args[i]);
        		}
        		else {
        			JavaRDD<String> localLineItems = context
                            .textFile(args[i]);
        			lineItems = lineItems.union(localLineItems);
        		}
        	}
            
			JavaRDD<Tuple5> rawPatterns = lineItems
                    .flatMap(new FlatMapFunction<String, Tuple5>() {
                        @Override
                        public Iterable<Tuple5> call(String sentence) throws Exception {
                            //Create regex pattern that finds NER XML tags in the sentence (e.g. "<LOCATION>New York</LOCATION>")
                            Pattern NERTagPattern = Pattern.compile("<([A-Z]+)>(.+?)</([A-Z]+)>");
                            Matcher NERMatcher = NERTagPattern.matcher(sentence);

                            //Store all tokens in a list of 2-tuples <string, NER tag>
                            List<Tuple2> tokenList = new ArrayList();
                            Integer lastIndex = 0;
                            //Iterate through all occurences of the regex pattern
                            while (NERMatcher.find()) {
                                //First, add the normal words (i.e. w/o NER tags) to the token list
                                //Add them as 2-tuples <string, "">
                                String stringBefore = sentence.substring(lastIndex, NERMatcher.start());
                                String[] splittedStringBefore = stringBefore.split(" ");
                                for (String word : splittedStringBefore) {
                                    if (!word.isEmpty()) {
                                        tokenList.add(new Tuple2(word, ""));
                                    }
                                }

                                //Then, add the NER-tagged tokens to the token list
                                //Add them as 2 tuples <string, NER tag>
                                tokenList.add(new Tuple2(NERMatcher.group(2), NERMatcher.group(1)));

                                //Remember last processed character
                                lastIndex = NERMatcher.end();
                            }
                            //Lastly, add the normal words (i.e. w/o NER tags) after the last NER tag
                            //Add them as 2-tuples <string, "">
                            String endString = sentence.substring(lastIndex, sentence.length());
                            String[] splittedEndString = endString.split(" ");
                            for (String word : splittedEndString) {
                                if (!word.isEmpty()) {
                                    tokenList.add(new Tuple2(word, ""));
                                }
                            }

                            /*
                            Now, the token list look like this:
                            <"Goldman Sachs", "ORGANIZATION">
                            <"is", "">
                            <"headquarted", "">
                            <"in", "">
                            <"New York City", "LOCATION"
                            */

                            List patterns = new ArrayList();
                            //For each of the seed tuples <A, B>:
                            for (Tuple2 seedTuple : task_seedTuples) {

                                //Take note of where A and B appeared in the sentence (and with the right NER tags)
                                List<Integer> entity0sites = new ArrayList<Integer>();
                                List<Integer> entity1sites = new ArrayList<Integer>();
                                Integer tokenIndex = 0;
                                for (Tuple2<String, String> wordEntity : tokenList) {
                                    String word = wordEntity._1();
                                    String entity = wordEntity._2();

                                    if (word.equals(seedTuple._1()) && entity.equals(task_entityTags.get(0))) {
                                        entity0sites.add(tokenIndex);
                                    } else if (word.equals(seedTuple._2()) && entity.equals(task_entityTags.get(1))) {
                                        entity1sites.add(tokenIndex);
                                    }
                                    tokenIndex++;
                                }

                                //For each pair of A and B in the sentence, generate a pattern and add it to the list
                                for (Integer entity0site : entity0sites) {
                                    for (Integer entity1site : entity1sites) {
                                        Integer windowSize = 5;
                                        Integer maxDistance = 5;
                                        if (entity0site < entity1site && (entity1site - entity0site) <= maxDistance) {
                                            Map beforeContext = produceContext(tokenList.subList(Math.max(0, entity0site - windowSize), entity0site));
                                            Map betweenContext = produceContext(tokenList.subList(entity0site + 1, entity1site));
                                            Map afterContext = produceContext(tokenList.subList(entity1site + 1, Math.min(tokenList.size(), entity1site + windowSize + 1)));
                                            Tuple5 pattern = new Tuple5(beforeContext, task_entityTags.get(0), betweenContext, task_entityTags.get(1), afterContext);
                                            patterns.add(pattern);
                                        } else if (entity1site < entity0site && (entity0site - entity1site) <= maxDistance) {
                                            Map beforeContext = produceContext(tokenList.subList(Math.max(0, entity1site - windowSize), entity1site));
                                            Map betweenContext = produceContext(tokenList.subList(entity1site + 1, entity0site));
                                            Map afterContext = produceContext(tokenList.subList(entity0site + 1, Math.min(tokenList.size(), entity0site + windowSize + 1)));
                                            Tuple5 pattern = new Tuple5(beforeContext, task_entityTags.get(1), betweenContext, task_entityTags.get(0), afterContext);
                                            patterns.add(pattern);
                                        }
                                    }
                                }
                            }

                            return patterns;
                        }
                    });

            rawPatterns.saveAsTextFile(outputFile+"/patterns");
            System.out.println("Fertisch!");

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
