package de.hpi.fgis.dbda.textmining.maintask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class App
{

    //Parameters

    private static Integer numberOfIterations = 1;
    //Maximum size of the left window (left of first entity tag) and the right window (right of second entity tag)
    private static Integer windowSize = 5;
    //Maximum distance between both entity tags in tokens
    private static Integer maxDistance = 5;
    //Similarity threshold for clustering of patterns
    private static Float similarityThreshold = 0.5f;
    //Minimal degree of match for a pattern to match a text segment
    private static Float degreeOfMatchThreshold = 0.95f;
    private static Integer minimalClusterSize = 5;
    private static Float tupleConfidenceThreshold = 0.99f;

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

    private static TupleContext calculateCentroid(List<TupleContext> patterns) {
        Map<String, Float> leftCounter = new LinkedHashMap();
        Map<String, Float>  middleCounter = new LinkedHashMap();
        Map<String, Float>  rightCounter = new LinkedHashMap();

        String leftEntity = patterns.get(0)._2();
        String rightEntity = patterns.get(0)._4();

        //Add up all contexts
        for (TupleContext pattern : patterns) {
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

        return new TupleContext(leftCounter, leftEntity, middleCounter, rightEntity, rightCounter);
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
    
    private static Float calculateDegreeOfMatch(TupleContext pattern, TupleContext tuple) {
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

    private static Float calculateDegreeOfMatchWithCluster(TupleContext pattern, List<TupleContext> cluster) {
    	TupleContext centroid = calculateCentroid(cluster);
        return calculateDegreeOfMatch(pattern, centroid);
    }

    private static List<List> clusterPatterns(List<TupleContext> patternList) {
        List<List> clusters = new ArrayList<>();
        for (TupleContext pattern : patternList) {
            if (clusters.isEmpty()) {
                List<TupleContext> newCluster = new ArrayList<>();
                newCluster.add(pattern);
                clusters.add(newCluster);
            } else {
                Integer clusterIndex = 0;
                Integer nearestCluster = null;
                Float greatestSimilarity = 0.0f;
                for (List<TupleContext> cluster : clusters) {
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
                    List<TupleContext> separateCluster = new ArrayList<>();
                    separateCluster.add(pattern);
                    clusters.add(separateCluster);
                }
            }
        }
        return clusters;
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

    public static void main( String[] args )
    {

        final String outputDirectory = args[0];
        
        //Initialize entity tags for the relation extraction
        final List<String> task_entityTags = new ArrayList<>();
        
        task_entityTags.add("ORGANIZATION");
        task_entityTags.add("LOCATION");

        //Initialize spark environment
        SparkConf config = new SparkConf().setAppName(App.class.getName());
        config.set("spark.hadoop.validateOutputSpecs", "false");

        try(JavaSparkContext context = new JavaSparkContext(config)) {
        	
        	//Read sentences
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
            
            //Filter sentences: retain only those that contain both entity tags
            JavaRDD<String> sentencesWithTags = lineItems.filter(new Function<String, Boolean>() {
				
				@Override
				public Boolean call(String v1) throws Exception {
					return v1.contains(task_entityTags.get(0)) && v1.contains(task_entityTags.get(1));
				}
			});

            //Generate a mapping <organization, sentence>
            JavaPairRDD<String, String> organizationKeyList = sentencesWithTags
                    .flatMapToPair(new PairFlatMapFunction<String, String, String>() {

                        @Override
                        public Iterable<Tuple2<String, String>> call(String sentence)
                                throws Exception {
                            List<Tuple2<String, String>> keyList = new ArrayList();
                            Pattern NERTagPattern = Pattern.compile("<ORGANIZATION>(.+?)</ORGANIZATION>");
                            Matcher NERMatcher = NERTagPattern.matcher(sentence);
                            while (NERMatcher.find()) {
                                keyList.add(new Tuple2(NERMatcher.group(1), sentence));
                            }
                            return keyList;
                        }
                    });
            
            organizationKeyList.persist(StorageLevel.MEMORY_ONLY());

            //Read the seed tuples as pairs: <organization, location>
            JavaPairRDD<String, String> seedTuples = context.textFile(args[1])
            		.mapToPair(new PairFunction<String, String, String>() {

						@Override
						public Tuple2<String, String> call(String t)
								throws Exception {
							SeedTuple st = new SeedTuple(t);
							return new Tuple2<String, String>(st.ORGANIZATION, st.LOCATION);
						}
					});

            System.out.println("#########################");
            System.out.println("#Finished initialization#");
            System.out.println("#########################");

            Integer currentIteration = 0;

            while (currentIteration <= numberOfIterations) {
                currentIteration += 1;
                System.out.println("#########################");
                System.out.println("####Begin iteration " + currentIteration + "####");
                System.out.println("#########################");

                //Retain only those sentences with a organization from the seed tuples: <organization, <sentence, location>>
                JavaPairRDD<String, Tuple2<String, String>> organizationKeyListJoined = organizationKeyList.join(seedTuples);

                //Search the sentences for raw patterns
                JavaRDD<TupleContext> rawPatterns = organizationKeyListJoined
                        .flatMap(new FlatMapFunction<Tuple2<String, Tuple2<String, String>>, TupleContext>() {

                            @Override
                            public Iterable<TupleContext> call(
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
                                            TupleContext pattern = new TupleContext(beforeContext, task_entityTags.get(0), betweenContext, task_entityTags.get(1), afterContext);
                                            patterns.add(pattern);
                                        } else if (entity1site < entity0site && (entity0site - entity1site) <= maxDistance) {
                                            Map beforeContext = produceContext(tokenList.subList(Math.max(0, entity1site - windowSize), entity1site));
                                            Map betweenContext = produceContext(tokenList.subList(entity1site + 1, entity0site));
                                            Map afterContext = produceContext(tokenList.subList(entity0site + 1, Math.min(tokenList.size(), entity0site + windowSize + 1)));
                                            TupleContext pattern = new TupleContext(beforeContext, task_entityTags.get(1), betweenContext, task_entityTags.get(0), afterContext);
                                            patterns.add(pattern);
                                        }
                                    }
                                }
                                return patterns;
                            }
                        });
                
                System.out.println("#########################");
                System.out.println("Raw Patterns found: "+rawPatterns.count());
                System.out.println("#########################");

                //Collect all raw patterns on the driver
                List<TupleContext> patternList = rawPatterns
                        .collect();

                //Cluster patterns with a single-pass clustering algorithm
                List<List> clusters = clusterPatterns(patternList);
                
                System.out.println("#########################");
                System.out.println("Clusters found: "+clusters.size());
                System.out.println("#########################");

                //Remove clusters with less than 5 patterns?!
                final List<TupleContext> patterns = new ArrayList();
                for (List<TupleContext> l : clusters) {
                    if (l.size() >= minimalClusterSize) {
                        TupleContext centroid = calculateCentroid(l);
                        patterns.add(centroid);
                    }
                }
                
                System.out.println("#########################");
                System.out.println("Patterns found: "+patterns.size());
                System.out.println("#########################");

                //System.out.println(patterns);

                //Search sentences for occurrences of the two entity tags
                //Returns: List of <tuple, context>
                JavaRDD<Tuple2<Tuple2, TupleContext>> textSegments = sentencesWithTags
                        .flatMap(new FlatMapFunction<String, Tuple2<Tuple2, TupleContext>>() {
                            @Override
                            public Iterable<Tuple2<Tuple2, TupleContext>> call(String sentence) throws Exception {

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
                                List<Tuple2<Tuple2, TupleContext>> textSegmentList = new ArrayList<>();
                                for (Integer entity0site : entity0sites) {
                                    for (Integer entity1site : entity1sites) {

                                        if (entity0site < entity1site && (entity1site - entity0site) <= maxDistance) {
                                            Map beforeContext = produceContext(tokenList.subList(Math.max(0, entity0site - windowSize), entity0site));
                                            Map betweenContext = produceContext(tokenList.subList(entity0site + 1, entity1site));
                                            Map afterContext = produceContext(tokenList.subList(entity1site + 1, Math.min(tokenList.size(), entity1site + windowSize + 1)));
                                            textSegmentList.add(new Tuple2(new Tuple2(tokenList.get(entity0site)._1(), tokenList.get(entity1site)._1()), new TupleContext(beforeContext, task_entityTags.get(0), betweenContext, task_entityTags.get(1), afterContext)));
                                        } else if (entity1site < entity0site && (entity0site - entity1site) <= maxDistance) {
                                            Map beforeContext = produceContext(tokenList.subList(Math.max(0, entity1site - windowSize), entity1site));
                                            Map betweenContext = produceContext(tokenList.subList(entity1site + 1, entity0site));
                                            Map afterContext = produceContext(tokenList.subList(entity0site + 1, Math.min(tokenList.size(), entity0site + windowSize + 1)));
                                            textSegmentList.add(new Tuple2(new Tuple2(tokenList.get(entity0site)._1(), tokenList.get(entity1site)._1()), new TupleContext(beforeContext, task_entityTags.get(1), betweenContext, task_entityTags.get(0), afterContext)));
                                        }
                                    }
                                }
                                return textSegmentList;
                            }
                        });

                textSegments.persist(StorageLevel.MEMORY_ONLY());
                //textSegments.saveAsTextFile(outputDirectory + "/textsegments");

                //Generate <organization, <pattern_id, location>> when the pattern generated the tuple
                JavaPairRDD<String, Tuple2<Integer, String>> organizationsWithMatchedLocations = textSegments
                        .flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2, TupleContext>, String, Tuple2<Integer, String>>() {
                            @Override
                            public Iterable<Tuple2<String, Tuple2<Integer, String>>> call(Tuple2<Tuple2, TupleContext> textSegment) throws Exception {

                                //Algorithm from figure 4
                                List<Tuple2<String, Tuple2<Integer, String>>> generatedTuples = new ArrayList();
                                TupleContext tupleContext = textSegment._2();

                                Integer patternIndex = 0;
                                while (patternIndex < patterns.size()) {
                                    TupleContext pattern = patterns.get(patternIndex);
                                    float similarity = calculateDegreeOfMatch(tupleContext, pattern);
                                    if (similarity >= degreeOfMatchThreshold) {
                                        generatedTuples.add(new Tuple2(textSegment._1()._1(), new Tuple2(patternIndex, textSegment._1()._2())));
                                    }
                                    patternIndex++;
                                }

                                return generatedTuples;
                            }
                        });

                //Join location from seed tuples onto the matched locations: <organization, <<pattern_id, matched_location>, seedtuple_location>>
                JavaPairRDD<String, Tuple2<Tuple2<Integer, String>, String>> organizationsWithMatchedAndCorrectLocation = organizationsWithMatchedLocations
                        .join(seedTuples);

                //Return counts of positives and negatives depending on whether matched location equals seed tuple location: <pattern_id, <#positives, #negatives>>
                JavaPairRDD<Integer, Tuple2<Integer, Integer>> patternsWithPositiveAndNegatives = organizationsWithMatchedAndCorrectLocation
                        .mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<Integer, String>, String>>, Integer, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<Tuple2<Integer, String>, String>> organizationWithMatchedAndCorrectLocation) throws Exception {
                                Integer patternID = organizationWithMatchedAndCorrectLocation._2()._1()._1();
                                String matchedLocation = organizationWithMatchedAndCorrectLocation._2()._1()._2();
                                String correctLocation = organizationWithMatchedAndCorrectLocation._2()._2();
                                if (matchedLocation.equals(correctLocation)) {
                                    return new Tuple2(patternID, new Tuple2<>(1, 0));
                                } else {
                                    return new Tuple2(patternID, new Tuple2<>(0, 1));
                                }
                            }
                        });

                //Sum up counts of positives and negatives for each pattern id: <pattern_id, <#positives, #negatives>>
                JavaPairRDD<Integer, Tuple2<Integer, Integer>> patternsWithSummedUpPositiveAndNegatives = patternsWithPositiveAndNegatives
                        .reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> posNeg1, Tuple2<Integer, Integer> posNeg2) throws Exception {
                                Integer sumOfPositives = posNeg1._1() + posNeg2._1();
                                Integer sumOfNegatives = posNeg1._2() + posNeg2._2();
                                return new Tuple2(sumOfPositives, sumOfNegatives);
                            }
                        });

                //Calculate pattern confidence: <pattern_id, confidence>
                JavaPairRDD<Integer, Float> patternConfidences = patternsWithSummedUpPositiveAndNegatives
                        .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>, Integer, Float>() {
                            @Override
                            public Tuple2<Integer, Float> call(Tuple2<Integer, Tuple2<Integer, Integer>> patternWithSummedUpPositiveAndNegatives) throws Exception {
                                Integer patternID = patternWithSummedUpPositiveAndNegatives._1();
                                Integer positives = patternWithSummedUpPositiveAndNegatives._2()._1();
                                Integer negatives = patternWithSummedUpPositiveAndNegatives._2()._2();
                                Float confidence = (float) positives / (positives + negatives);
                                return new Tuple2(patternID, confidence);
                            }
                        });

                //patternConfidences.saveAsTextFile(outputDirectory + "/patternconfidences");

                //Compile candidate tuple list: <pattern, <candidate tuple, similarity>>
                JavaPairRDD<Integer, Tuple2<Tuple2, Float>> patternsWithTuples = textSegments
                        .flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2, TupleContext>, Integer, Tuple2<Tuple2, Float>>() {
                            @Override
                            public Iterable<Tuple2<Integer, Tuple2<Tuple2, Float>>> call(Tuple2<Tuple2, TupleContext> textSegment) throws Exception {

                                //Algorithm from figure 4
                                //
                                List<Tuple2<Integer, Tuple2<Tuple2, Float>>> candidateTuplesWithPatternAndSimilarity = new ArrayList();

                                Tuple2<String, String> candidateTuple = textSegment._1();
                                TupleContext tupleContext = textSegment._2();

                                Integer bestPattern = null;
                                float bestSimilarity = 0.0f;
                                Integer patternIndex = 0;
                                while (patternIndex < patterns.size()) {
                                    TupleContext pattern = patterns.get(patternIndex);
                                    float similarity = calculateDegreeOfMatch(tupleContext, pattern);
                                    if (similarity >= degreeOfMatchThreshold) {
                                        if (similarity > bestSimilarity) {
                                            bestSimilarity = similarity;
                                            bestPattern = patternIndex;
                                        }
                                    }
                                    patternIndex++;
                                }
                                if (bestSimilarity >= degreeOfMatchThreshold) {
                                    candidateTuplesWithPatternAndSimilarity.add(new Tuple2(bestPattern, new Tuple2(candidateTuple, bestSimilarity)));
                                }
                                return candidateTuplesWithPatternAndSimilarity;
                            }
                        });

                //patternsWithTuples.saveAsTextFile(outputDirectory + "/patternsWithTuples" + currentIteration);

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
                
                System.out.println("#########################");
                System.out.println("Candidate Tuples found: "+candidateTuples.count());
                System.out.println("#########################");

                //Execute first step of tuple confidence calculation
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

                //Finish tuple confidence calculation, with organization as key: <organization, <location, tuple confidence>>
                JavaPairRDD<String, Tuple2<String, Float>> confidences = confidenceSubtrahend
                        .mapToPair(new PairFunction<Tuple2<Tuple2, Float>, String, Tuple2<String, Float>>() {
                            @Override
                            public Tuple2<String, Tuple2<String, Float>> call(Tuple2<Tuple2, Float> tupleAndSubtrahend) throws Exception {
                                String organization = (String) tupleAndSubtrahend._1()._1();
                                String location = (String) tupleAndSubtrahend._1()._2();
                                Float subtrahend = tupleAndSubtrahend._2();
                                return new Tuple2(organization, new Tuple2(location, 1.0f - subtrahend));
                            }
                        });

                //Filter candidate tuples by their confidence: <organization, <location, tuple confidence>>
                JavaPairRDD<String, Tuple2<String, Float>> filteredTuples = confidences
                        .filter(new Function<Tuple2<String, Tuple2<String, Float>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<String, Tuple2<String, Float>> tupleWithConfidence) throws Exception {
                                Float confidence = tupleWithConfidence._2()._2();
                                if (confidence > tupleConfidenceThreshold) {
                                    return true;
                                } else {
                                    return false;
                                }
                            }
                        });

                //Filter candidate tuples by organization, choosing highest confidence: <organization, <location, tuple confidence>>
                JavaPairRDD<String, Tuple2<String, Float>> uniqueFilteredTuples = filteredTuples
                        .reduceByKey(new Function2<Tuple2<String, Float>, Tuple2<String, Float>, Tuple2<String, Float>>() {
                            @Override
                            public Tuple2 call(Tuple2<String, Float> v1, Tuple2<String, Float> v2) throws Exception {
                                return v1._2() > v2._2() ? v1 : v2;
                            }
                        });

                //Store new seed tuples without their confidence: <organization, location>
                JavaPairRDD<String, String> newSeedTuples = uniqueFilteredTuples
                        .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Float>>, String, String>() {

                            @Override
                            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Float>> t) throws Exception {
                                String organization = t._1();
                                String location = t._2()._1();
                                return new Tuple2(organization, location);
                            }
                        });
                
                System.out.println("#########################");
                System.out.println("Seed Tuples found: "+newSeedTuples.count());
                System.out.println("#########################");

                //Add new seed tuples to the old ones
                seedTuples = seedTuples.union(newSeedTuples);

                seedTuples.saveAsTextFile(outputDirectory + "/newseedtuples" + currentIteration);
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
