package de.hpi.fgis.dbda.textmining.maintask;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import scala.Tuple2;
import scala.Tuple5;
import edu.stanford.nlp.util.CoreMap;

public class App
{
	
	private static Float similarityThreshold = 0.5f;
	
	private static int supportThreshold = 5;

    private static Map produceContext(List<Tuple2> tokenList) {
        /*
           Produce the context based on a given token list. A context is a HashMap that maps each token in the token
           list to a weight. The more prominent or frequent a token is, the higher is the associated weight.
         */
        Map<String, Integer> termCounts = new LinkedHashMap();

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
        Map<String, Float> context = new LinkedHashMap();
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
        Map<String, Float> leftCounter = new LinkedHashMap();
        Map<String, Float>  middleCounter = new LinkedHashMap();
        Map<String, Float>  rightCounter = new LinkedHashMap();

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
    
    private static Float calculateDegreeOfMatch(Tuple5<Map, String, Map, String, Map> pattern, Tuple5<Map, String, Map, String, Map> tuple) {
    	Map<String, Float> centroidLeft = tuple._1();
        Map<String, Float> centroidMiddle = tuple._3();
        Map<String, Float> centroidRight = tuple._5();

        Map<String, Float> patternLeft = pattern._1();
        Map<String, Float> patternMiddle = pattern._3();
        Map<String, Float> patternRight = pattern._5();

        if (pattern._2().equals(tuple._2()) && pattern._4().equals(tuple._4())) {
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

    private static Float calculateDegreeOfMatchWithCluster(Tuple5<Map, String, Map, String, Map> pattern, List<Tuple5<Map, String, Map, String, Map>> cluster) {
        Tuple5<Map, String, Map, String, Map> centroid = calculateCentroid(cluster);
        return calculateDegreeOfMatch(pattern, centroid);
    }

    private static List<Tuple2> generateTokenList(String sentence) {
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
		return tokenList;
	}

	private static Float calculatePatternConfidence(
			Iterable<Tuple2> tuplesFromPattern, List<Tuple2> task_seedTuples) {
		float positives = 0.0f;
		float negatives = 0.0f;
		for (Tuple2 tupleFromPattern : tuplesFromPattern) {
			for (Tuple2 knownTuple : task_seedTuples) {
				if (knownTuple._1.equals(tupleFromPattern._1)) {
					if (knownTuple._2.equals(tupleFromPattern._2)) {
						positives += 1.0f;
					}
					else {
						negatives += 1.0f;
					}
					break;
				}
			}
		}
		return positives/(positives+negatives);
	}

    public static void main( String[] args )
    {

        final String outputFile = args[0];
        
        //Initialize entity tags for the relation extraction
        final List<String> task_entityTags = new ArrayList<>();

        //Initialize list of seed tuples
        final List<Tuple2> task_seedTuples = new ArrayList<>();
        
        task_entityTags.add("ORGANIZATION");
        task_entityTags.add("LOCATION");
        
        task_seedTuples.add(new Tuple2("Microsoft", "Redmond"));
        task_seedTuples.add(new Tuple2("Google", "Palo Alto"));
        task_seedTuples.add(new Tuple2("Apple", "Cupertino"));
        task_seedTuples.add(new Tuple2("Exxon Corporation", "Irving"));

        //Initialize spark environment
        SparkConf config = new SparkConf().setAppName(App.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
        	
        	JavaRDD<String> lineItems = null;
        	
        	for (int i = 2; i < args.length; i++) {
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
            

            JavaRDD<String> sentencesWithTags = lineItems.filter(new Function<String, Boolean>() {
				
				@Override
				public Boolean call(String v1) throws Exception {
					return v1.contains(task_entityTags.get(0)) && v1.contains(task_entityTags.get(1));
				}
			});
            
            JavaPairRDD<String, String> organizationKeyList = sentencesWithTags.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

				@Override
				public Iterable<Tuple2<String, String>> call(String sentence)
						throws Exception {
					List<Tuple2<String, String>> keyList = new ArrayList<Tuple2<String, String>>();
					Pattern NERTagPattern = Pattern.compile("<ORGANIZATION>(.+?)</ORGANIZATION>");
			        Matcher NERMatcher = NERTagPattern.matcher(sentence);
			        while (NERMatcher.find()) {
			        	keyList.add(new Tuple2<String, String>(NERMatcher.group(1), sentence));
			        }
					return keyList;
				}
			});
            
            organizationKeyList.persist(StorageLevel.MEMORY_ONLY());
            
            JavaPairRDD<String, String> seedTuples = context.textFile(args[1])
            		.mapToPair(new PairFunction<String, String, String>() {

						@Override
						public Tuple2<String, String> call(String t)
								throws Exception {
							SeedTuple st = new SeedTuple(t);
							return new Tuple2<String, String>(st.ORGANIZATION, st.LOCATION);
						}
					});
            
            JavaPairRDD<String, Tuple2<String, String>> organizationKeyListJoined = organizationKeyList.join(seedTuples);
            
            JavaRDD<Tuple5> rawPatterns = organizationKeyListJoined
            		.flatMap(new FlatMapFunction<Tuple2<String,Tuple2<String,String>>, Tuple5>() {

						@Override
						public Iterable<Tuple5> call(
								Tuple2<String, Tuple2<String, String>> t)
								throws Exception {
								
								String seedTupleOrg = t._1();
								String sentence = t._2()._1();
								String seedTupleLocation = t._2()._2();
							
							 	List<Tuple2> tokenList = generateTokenList(sentence);
	                            
	                            /*
	                            Now, the token list look like this:
	                            <"Goldman Sachs", "ORGANIZATION">
	                            <"is", "">
	                            <"headquarted", "">
	                            <"in", "">
	                            <"New York City", "LOCATION"
	                            */
							 	
	                            List patterns = new ArrayList();
	                            
	                            //Take note of where A and B appeared in the sentence (and with the right NER tags)
	                            List<Integer> entity0sites = new ArrayList<Integer>();
	                            List<Integer> entity1sites = new ArrayList<Integer>();
	                            Integer tokenIndex = 0;
	                            for (Tuple2<String, String> wordEntity : tokenList) {
	                                String word = wordEntity._1();
	                                String entity = wordEntity._2();

	                                if (word.equals(seedTupleOrg) && entity.equals(task_entityTags.get(0))) {
	                                    entity0sites.add(tokenIndex);
	                                } else if (word.equals(seedTupleLocation) && entity.equals(task_entityTags.get(1))) {
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
							return patterns;
						}
					});

            List<Tuple5> patternList = rawPatterns
                    .collect();

            //Cluster patterns
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
                        Float similarity = calculateDegreeOfMatchWithCluster(pattern, cluster);
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
            
            // List of <Pattern, List of Tuples>
            
            final List<Tuple5<Map, String, Map, String, Map>> patterns = new ArrayList();

            //Remove clusters with less than 5 patterns?!
            for (List<Tuple5<Map, String, Map, String, Map>> l : clusters) {
            	if (l.size() > 5) {
	            	Tuple5 centroid = calculateCentroid(l);
	            	patterns.add(centroid);
            	}
            }

            //Search sentences for NER tags -> make textSegment list
            JavaRDD<List<Tuple2<Tuple2, Tuple5>>> textSegments = lineItems
                    .map(new Function<String, List<Tuple2<Tuple2, Tuple5>>>() {
                        @Override
                        public List<Tuple2<Tuple2, Tuple5>> call(String sentence) throws Exception {
                            List<Tuple2> tokenList = generateTokenList(sentence);

                            List<Integer> entity0sites = new ArrayList<Integer>();
                            List<Integer> entity1sites = new ArrayList<Integer>();
                            Integer tokenIndex = 0;
                            for (Tuple2<String, String> wordEntity : tokenList) {
                                String entity = wordEntity._2();

                                if (entity.equals(task_entityTags.get(0))) {
                                    entity0sites.add(tokenIndex);
                                } else if (entity.equals(task_entityTags.get(1))) {
                                    entity1sites.add(tokenIndex);
                                }
                                tokenIndex++;
                            }

                            //For each pair of A and B in the sentence, generate a text segment and add it to the list
                            List<Tuple2<Tuple2, Tuple5>> textSegmentList = new ArrayList<>();
                            for (Integer entity0site : entity0sites) {
                                for (Integer entity1site : entity1sites) {
                                    Integer windowSize = 5;
                                    Integer maxDistance = 5;
                                    if (entity0site < entity1site && (entity1site - entity0site) <= maxDistance) {
                                        Map beforeContext = produceContext(tokenList.subList(Math.max(0, entity0site - windowSize), entity0site));
                                        Map betweenContext = produceContext(tokenList.subList(entity0site + 1, entity1site));
                                        Map afterContext = produceContext(tokenList.subList(entity1site + 1, Math.min(tokenList.size(), entity1site + windowSize + 1)));
                                        textSegmentList.add(new Tuple2<Tuple2, Tuple5>(new Tuple2(tokenList.get(entity0site), tokenList.get(entity1site)), new Tuple5(beforeContext, task_entityTags.get(0), betweenContext, task_entityTags.get(1), afterContext)));
                                    } else if (entity1site < entity0site && (entity0site - entity1site) <= maxDistance) {
                                        Map beforeContext = produceContext(tokenList.subList(Math.max(0, entity1site - windowSize), entity1site));
                                        Map betweenContext = produceContext(tokenList.subList(entity1site + 1, entity0site));
                                        Map afterContext = produceContext(tokenList.subList(entity0site + 1, Math.min(tokenList.size(), entity0site + windowSize + 1)));
                                        textSegmentList.add(new Tuple2<Tuple2, Tuple5>(new Tuple2(tokenList.get(entity0site), tokenList.get(entity1site)), new Tuple5(beforeContext, task_entityTags.get(1), betweenContext, task_entityTags.get(0), afterContext)));
                                    }
                                }
                            }
                            return textSegmentList;
                        }
                    });

            //collect all tuples generated by each pattern
            JavaPairRDD<Integer, Tuple2> generatedTuples = textSegments
                    .flatMapToPair(new PairFlatMapFunction<List<Tuple2<Tuple2, Tuple5>>, Integer, Tuple2>() {
                        @Override
                        public Iterable<Tuple2<Integer, Tuple2>> call(List<Tuple2<Tuple2, Tuple5>> listOfTextSegments) throws Exception {

                            //Algorithm from figure 4
                            List<Tuple2<Integer, Tuple2>> generatedTuples = new ArrayList();
                            for (Tuple2<Tuple2, Tuple5> textSegment : listOfTextSegments) {

                                Tuple2<String, String> candidateTuple = textSegment._1();
                                Tuple5<Map, String, Map, String, Map> tupleContext = textSegment._2();

                                Integer patternIndex = 0;
                                while (patternIndex < patterns.size()) {
                                    Tuple5<Map, String, Map, String, Map> pattern = patterns.get(patternIndex);
                                    float similarity = calculateDegreeOfMatch(tupleContext, pattern);
                                    if (similarity >= similarityThreshold) {
                                        generatedTuples.add(new Tuple2(patternIndex, candidateTuple));
                                    }
                                    patternIndex++;
                                }
                            }
                            return generatedTuples;
                        }
                    });

            //TODO: do it with reducebykey()
            JavaPairRDD<Integer, Iterable<Tuple2>> tuplesGeneratedFromPattern_grouped = generatedTuples
                    .groupByKey();

            //calculate pattern confidences to JavaPairRDD: <pattern_id, confidence>
            JavaPairRDD<Integer, Float> patternConfidences = tuplesGeneratedFromPattern_grouped
                    .mapToPair(new PairFunction<Tuple2<Integer, Iterable<Tuple2>>, Integer, Float>() {
                        @Override
                        public Tuple2<Integer, Float> call(Tuple2<Integer, Iterable<Tuple2>> grouping) throws Exception {
                            Integer pattern = grouping._1();
                            Iterable<Tuple2> tuples = grouping._2();
                            Float confidence = calculatePatternConfidence(tuples, task_seedTuples);
                            return new Tuple2<>(pattern, confidence);
                        }
                    });

            //Compile candidate list: <pattern, <candidate tuple, similarity>>
            JavaPairRDD<Integer, Tuple2<Tuple2, Float>> patternsWithTuples = textSegments
                    .flatMapToPair(new PairFlatMapFunction<List<Tuple2<Tuple2, Tuple5>>, Integer, Tuple2<Tuple2, Float>>() {
                        @Override
                        public Iterable<Tuple2<Integer, Tuple2<Tuple2, Float>>> call(List<Tuple2<Tuple2, Tuple5>> textSegments) throws Exception {

                            //Algorithm from figure 4
                            //
                            List<Tuple2<Integer, Tuple2<Tuple2, Float>>> candidateTuplesWithPatternAndSimilarity = new ArrayList();
                            for (Tuple2<Tuple2, Tuple5> textSegment : textSegments) {

                                Tuple2<String, String> candidateTuple = textSegment._1();
                                Tuple5<Map, String, Map, String, Map> tupleContext = textSegment._2();

                                Integer bestPattern = null;
                                float bestSimilarity = 0.0f;
                                Integer patternIndex = 0;
                                while (patternIndex < patterns.size()) {
                                    Tuple5<Map, String, Map, String, Map> pattern = patterns.get(patternIndex);
                                    float similarity = calculateDegreeOfMatch(tupleContext, pattern);
                                    if (similarity >= similarityThreshold) {
                                        if (similarity > bestSimilarity) {
                                            bestSimilarity = similarity;
                                            bestPattern = patternIndex;
                                        }
                                    }
                                    patternIndex++;
                                }
                                if (bestSimilarity >= similarityThreshold) {
                                    candidateTuplesWithPatternAndSimilarity.add(new Tuple2(bestPattern, new Tuple2(candidateTuple, bestSimilarity)));
                                }
                            }
                            return candidateTuplesWithPatternAndSimilarity;
                        }
                    });

            //Join candidate tuples with pattern confidences: <pattern_id, <<candidate tuple, similarity>, pattern_conf>>
            JavaPairRDD<Integer, Tuple2<Tuple2<Tuple2, Float>, Float>> candidateTuplesWithPatternConfidences =
                    patternsWithTuples.join(patternConfidences);

            //Reformat to <candidate tuple, <pattern_conf, similarity>>
            JavaPairRDD<Tuple2, Tuple2<Float, Float>> candidateTuples = candidateTuplesWithPatternConfidences
                    .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Tuple2<Tuple2, Float>, Float>>, Tuple2, Tuple2<Float, Float>>() {
                        @Override
                        public Tuple2<Tuple2, Tuple2<Float, Float>> call(Tuple2<Integer, Tuple2<Tuple2<Tuple2, Float>, Float>> foo) throws Exception {
                            Tuple2 candidateTuple = foo._2()._1()._1();
                            Float patternConf = foo._2()._2();
                            Float similarity = foo._2()._1()._2();
                            return new Tuple2(candidateTuple, new Tuple2(patternConf, similarity));
                        }
                    });


            JavaPairRDD<Tuple2, Float> confidenceSubtrahend = candidateTuples
                    .combineByKey(
                            new Function<Tuple2<Float, Float>, Float>() {
                                @Override
                                public Float call(Tuple2<Float, Float> patternMatch) throws Exception {
                                    return 1.0f - (patternMatch._1() * patternMatch._2());
                                }
                            },
                            new Function2<Float, Tuple2<Float, Float>, Float>() {
                                @Override
                                public Float call(Float currentValue, Tuple2<Float, Float> patternMatch) throws Exception {
                                    return currentValue * (1.0f - (patternMatch._1() * patternMatch._2()));
                                }
                            }, new Function2<Float, Float, Float>() {
                                @Override
                                public Float call(Float value1, Float value2) throws Exception {
                                    return value1 * value2;
                                }
                            });

            JavaPairRDD<Tuple2, Float> confidences = confidenceSubtrahend
                    .mapToPair(new PairFunction<Tuple2<Tuple2, Float>, Tuple2, Float>() {
                        @Override
                        public Tuple2<Tuple2, Float> call(Tuple2<Tuple2, Float> tupleAndSubtrahend) throws Exception {
                            return new Tuple2(tupleAndSubtrahend._1(), 1.0f - tupleAndSubtrahend._2());
                        }
                    });

            //TODO: filter candidate tuples based on confidence
            confidences.saveAsTextFile(outputFile + "/confidences");

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
	
	static class SeedTuple implements Serializable {
        String ORGANIZATION;
        String LOCATION;

        SeedTuple(String line) {
            String[] values = line.split("\t");
            ORGANIZATION = values[0];
            LOCATION = values[1];
        }
    }
}
