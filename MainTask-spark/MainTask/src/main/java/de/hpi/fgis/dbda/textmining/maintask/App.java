package de.hpi.fgis.dbda.textmining.maintask;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import scala.Tuple5;
import scala.Tuple2;
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
        Map<String, Integer> termCounts = new HashMap();

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
        Map<String, Float> context = new HashMap();
        for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
            context.put(entry.getKey(), (float) entry.getValue() / sumCounts);
        }
        return context;
    }

    private static float sumCollection(Collection<Float> col) {
        float sum = 0.0f;
        for (float o : col) {
            sum += o;
        }
        return sum;
    }

    private static Tuple5 calculateCentroid(List<Tuple5<Map, String, Map, String, Map>> patterns) {
        Map<String, Float> leftCounter = new HashMap();
        Map<String, Float>  middleCounter = new HashMap();
        Map<String, Float>  rightCounter = new HashMap();

        String leftEntity = patterns.get(0)._2();
        String rightEntity = patterns.get(0)._4();

        //Add up all contexts
        for (Tuple5<Map, String, Map, String, Map> pattern : patterns) {
            leftCounter = sumMaps(leftCounter, pattern._1());
            middleCounter = sumMaps(middleCounter, pattern._3());
            rightCounter = sumMaps(rightCounter, pattern._5());
        }

        //Normalize counters
        float leftSum = sumCollection(leftCounter.values());
        float middleSum = sumCollection(middleCounter.values());
        float rightSum = sumCollection(rightCounter.values());

        for (String key : leftCounter.keySet()) {
            leftCounter.put(key, leftCounter.get(key) / leftSum);
        }
        for (String key : middleCounter.keySet()) {
            middleCounter.put(key, middleCounter.get(key) / middleSum);
        }
        for (String key : rightCounter.keySet()) {
            rightCounter.put(key, rightCounter.get(key) / rightSum);
        }

        return new Tuple5(leftCounter, leftEntity, middleCounter, rightEntity, rightCounter);
    }

    private static Map sumMaps(Map<String, Float> map1, Map<String, Float> map2) {
        //Add all values of map2 to map1
        for (Map.Entry<String, Float> entry : map2.entrySet()) {
        	if (map1.containsKey(entry.getKey())) {
        		map1.put(entry.getKey(), map1.get(entry.getKey()) + entry.getValue());
        	} else {
        		map1.put(entry.getKey(), entry.getValue());
        	}
        }
        return map1;
    }

    private static Float calculateDegreeOfMatch(Tuple5<Map, String, Map, String, Map> pattern, List<Tuple5<Map, String, Map, String, Map>> cluster) {
        Tuple5<Map, String, Map, String, Map> centroid = calculateCentroid(cluster);
        Map<String, Float> centroidLeft = centroid._1();
        Map<String, Float> centroidMiddle = centroid._3();
        Map<String, Float> centroidRight = centroid._5();

        Map<String, Float> patternLeft = pattern._1();
        Map<String, Float> patternMiddle = pattern._3();
        Map<String, Float> patternRight = pattern._5();

        if (pattern._2().equals(centroid._2()) && pattern._4().equals(centroid._4())) {
            float leftSimilarity = 0;
            float middleSimilarity = 0;
            float rightSimilarity = 0;
            for (String key : patternLeft.keySet()) {
                if (centroidLeft.keySet().contains(key)) {
                    leftSimilarity += patternLeft.get(key) * centroidLeft.get(key);
                }
            }
            for (String key : patternMiddle.keySet()) {
                if (centroidMiddle.keySet().contains(key)) {
                    middleSimilarity += patternMiddle.get(key) * centroidMiddle.get(key);
                }
            }
            for (String key : patternRight.keySet()) {
                if (centroidRight.keySet().contains(key)) {
                    rightSimilarity += patternRight.get(key) * centroidRight.get(key);
                }
            }
            return leftSimilarity + middleSimilarity + rightSimilarity;
        } else {
            return 0.0f;
        }
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

            assert lineItems != null;
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

            List<Tuple5> patternList = rawPatterns
                    .collect();

            //Cluster patterns
            Float similarityThreshold = 0.5f;
            List<List> clusters = new ArrayList<>();
            for (Tuple5 pattern : patternList) {
                if (clusters.isEmpty()) {
                    List<Tuple5> newCluster = new ArrayList<>();
                    newCluster.add(pattern);
                    clusters.add(newCluster);
                } else {
                    Integer clusterIndex = 0;
                    Integer nearestCluster = null;
                    Float greatestSimilarity = 0.0f;
                    for (List<Tuple5<Map, String, Map, String, Map>> cluster : clusters) {
                        Float similarity = calculateDegreeOfMatch(pattern, cluster);
                        if (similarity > greatestSimilarity) {
                            nearestCluster = clusterIndex;
                            greatestSimilarity = similarity;
                        }
                        clusterIndex++;
                    }

                    if (greatestSimilarity > similarityThreshold) {
                        clusters.get(nearestCluster).add(pattern);
                    } else {
                        List<Tuple5> separateCluster = new ArrayList<>();
                        separateCluster.add(pattern);
                        clusters.add(separateCluster);
                    }
                }
            }
            
            // build one pattern from cluster (centroid)
            
            // evaluate patterns
            
            // add patterns to pattern list
            
            // search new (+ old) tuples using patterns
            
            // evaluate tuples
            
            // add tuples to tuple list
            
            // find new patterns using new tuples

            //rawPatterns.saveAsTextFile(outputFile+"/patterns");
            
            int i = 1;
            for (List<Tuple5<Map, String, Map, String, Map>> l : clusters) {
            	System.out.println("Cluster #" + i++);
            	for (Tuple5 t : l) {
            		System.out.println(t.toString());
            	}
            	System.out.println("Centroid: ");
            	System.out.println(calculateCentroid(l));
            }
            
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
