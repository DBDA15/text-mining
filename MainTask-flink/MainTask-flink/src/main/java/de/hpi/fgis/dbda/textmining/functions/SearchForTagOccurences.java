package de.hpi.fgis.dbda.textmining.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import de.hpi.fgis.dbda.textmining.MainTask_flink.ContextProducer;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TokenListGenerator;

public class SearchForTagOccurences implements
		FlatMapFunction<String, Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>>> {
	
	private List<String> task_entityTags;
	private int maxDistance;
	private int windowSize;

	public SearchForTagOccurences(List<String> task_entityTags, int maxDistance, int windowSize) {
		this.task_entityTags = task_entityTags;
		this.maxDistance = maxDistance;
		this.windowSize = windowSize;		
	}

	@Override
	public void flatMap(String arg0,
			Collector<Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>>> arg1) throws Exception {
		List<Tuple2> tokenList = TokenListGenerator.generateTokenList(arg0);

        List<Integer> entity0sites = new ArrayList<Integer>();
        List<Integer> entity1sites = new ArrayList<Integer>();
        Integer tokenIndex = 0;
        for (Tuple2<String, String> wordEntity : tokenList) {
            String entity = wordEntity.f1;

            if (entity.equals(task_entityTags.get(0))) {
                entity0sites.add(tokenIndex);
            } else if (entity.equals(task_entityTags.get(1))) {
                entity1sites.add(tokenIndex);
            }
            tokenIndex++;
        }

        //For each pair of A and B in the sentence, generate a text segment and add it to the list
        List<Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>>> textSegmentList = new ArrayList<>();
        for (Integer entity0site : entity0sites) {
            for (Integer entity1site : entity1sites) {

                if (entity0site < entity1site && (entity1site - entity0site) <= maxDistance) {
                    Map beforeContext = ContextProducer.produceContext(tokenList.subList(Math.max(0, entity0site - windowSize), entity0site));
                    Map betweenContext = ContextProducer.produceContext(tokenList.subList(entity0site + 1, entity1site));
                    Map afterContext = ContextProducer.produceContext(tokenList.subList(entity1site + 1, Math.min(tokenList.size(), entity1site + windowSize + 1)));
                    textSegmentList.add(new Tuple2(new Tuple2(tokenList.get(entity0site).f0, tokenList.get(entity1site).f0), new Tuple5<Map, String, Map, String, Map>(beforeContext, task_entityTags.get(0), betweenContext, task_entityTags.get(1), afterContext)));
                } else if (entity1site < entity0site && (entity0site - entity1site) <= maxDistance) {
                    Map beforeContext = ContextProducer.produceContext(tokenList.subList(Math.max(0, entity1site - windowSize), entity1site));
                    Map betweenContext = ContextProducer.produceContext(tokenList.subList(entity1site + 1, entity0site));
                    Map afterContext = ContextProducer.produceContext(tokenList.subList(entity0site + 1, Math.min(tokenList.size(), entity0site + windowSize + 1)));
                    textSegmentList.add(new Tuple2(new Tuple2(tokenList.get(entity0site).f0, tokenList.get(entity1site).f0), new Tuple5<Map, String, Map, String, Map>(beforeContext, task_entityTags.get(1), betweenContext, task_entityTags.get(0), afterContext)));
                }
            }
        }
        for (Tuple2<Tuple2<String, String>, Tuple5<Map, String, Map, String, Map>> tupleAndContext : textSegmentList) {
            arg1.collect(tupleAndContext);        	
        }

	}

}
