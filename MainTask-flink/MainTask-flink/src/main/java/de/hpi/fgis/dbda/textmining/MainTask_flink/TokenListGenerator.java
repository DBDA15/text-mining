package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.java.tuple.Tuple2;

public class TokenListGenerator {

    public static List<Tuple2<String, String>> generateTokenList(String sentence) {
        //Create regex pattern that finds NER XML tags in the sentence (e.g. "<LOCATION>New York</LOCATION>")
        Pattern NERTagPattern = Pattern.compile("<([A-Z]+)>(.+?)</([A-Z]+)>");
        Matcher NERMatcher = NERTagPattern.matcher(sentence);

        //Store all tokens in a list of 2-tuples <string, NER tag>
        List<Tuple2<String, String>> tokenList = new ArrayList();
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
	
}
