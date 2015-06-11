package de.hpi.fgis.dbda.textmining.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import de.hpi.fgis.dbda.textmining.MainTask_flink.App;
import de.hpi.fgis.dbda.textmining.MainTask_flink.ContextProducer;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TokenListGenerator;
import de.hpi.fgis.dbda.textmining.MainTask_flink.TupleContext;

public class SearchRawPatterns implements FlatMapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, TupleContext> {

    private List<String> entities;

    public SearchRawPatterns(List<String> entityTags) {
        super();
        this.entities = entityTags;
    }

	@Override
    public void flatMap(Tuple2<Tuple2<String, String>, Tuple2<String, String>> t, Collector<TupleContext> results) throws Exception {

		String seedTupleOrg = t.f0.f0;
		String sentence = t.f0.f1;
		String seedTupleLocation = t.f1.f1;

		List<Tuple2> tokenList = TokenListGenerator.generateTokenList(sentence);

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
			String word = wordEntity.f0;
			String entity = wordEntity.f1;

			if (word.equals(seedTupleOrg) && entity.equals(this.entities.get(0))) {
				entity0sites.add(tokenIndex);
			} else if (word.equals(seedTupleLocation) && entity.equals(this.entities.get(1))) {
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
					Map beforeContext = ContextProducer.produceContext(tokenList.subList(Math.max(0, entity0site - windowSize), entity0site));
					Map betweenContext = ContextProducer.produceContext(tokenList.subList(entity0site + 1, entity1site));
					Map afterContext = ContextProducer.produceContext(tokenList.subList(entity1site + 1, Math.min(tokenList.size(), entity1site + windowSize + 1)));
					TupleContext pattern = new TupleContext(beforeContext, this.entities.get(0), betweenContext, this.entities.get(1), afterContext);
					results.collect(pattern);
				} else if (entity1site < entity0site && (entity0site - entity1site) <= maxDistance) {
					Map beforeContext = ContextProducer.produceContext(tokenList.subList(Math.max(0, entity1site - windowSize), entity1site));
					Map betweenContext = ContextProducer.produceContext(tokenList.subList(entity1site + 1, entity0site));
					Map afterContext = ContextProducer.produceContext(tokenList.subList(entity0site + 1, Math.min(tokenList.size(), entity0site + windowSize + 1)));
					TupleContext pattern = new TupleContext(beforeContext, this.entities.get(1), betweenContext, this.entities.get(0), afterContext);
                    results.collect(pattern);
				}
			}
		}
	}

}
