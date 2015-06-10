package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import sun.misc.BASE64Encoder;

/**
 * Encodes an int array as a Base64 string.
 *
 * Created by basti on 6/6/15.
 */
public class ConvertIntArrayToString extends RichMapFunction<Tuple1<int[]>, Tuple1<String>> {

    /** Loaded after deployment to avoid serialization issues. */
    private BASE64Encoder base64Encoder;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.base64Encoder = new BASE64Encoder();
    }

    @Override
    public Tuple1<String> map(Tuple1<int[]> tuple1) throws Exception {
        // Transform int array into byte array.
        int[] ints = tuple1.f0;
        byte[] bytes = new byte[ints.length * 4];
        for (int i = 0; i < ints.length; i++) {
            bytes[4 * i] = (byte) (ints[i] >> 24);
            bytes[4 * i + 1] = (byte) (ints[i] >> 16);
            bytes[4 * i + 2] = (byte) (ints[i] >> 8);
            bytes[4 * i + 3] = (byte) (ints[i] >> 0);
        }

        // Encode byte array as Base 64.
        return new Tuple1<>(this.base64Encoder.encode(bytes));
    }
}
