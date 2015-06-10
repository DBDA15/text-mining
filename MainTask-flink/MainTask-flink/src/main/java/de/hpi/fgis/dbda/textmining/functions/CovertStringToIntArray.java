package de.hpi.fgis.dbda.textmining.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import sun.misc.BASE64Decoder;

/**
 * Decodes an int array from a Base64 string.
 *
 * Created by basti on 6/6/15.
 */
public class CovertStringToIntArray extends RichMapFunction<Tuple1<String>, Tuple1<int[]>> {

    /** Loaded after deployment to avoid serialization issues. */
    private BASE64Decoder base64Decoder;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.base64Decoder = new BASE64Decoder();
    }

    @Override
    public Tuple1<int[]> map(Tuple1<String> base64) throws Exception {
        // Decode Base64.
        byte[] bytes = this.base64Decoder.decodeBuffer(base64.f0);

        // Transform byte array into an int array.
        if ((bytes.length & 0x2) != 0) {
            throw new IllegalStateException(String.format("Byte array length should be divisible by 4. (is %d)", bytes.length));
        }
        int[] ints = new int[bytes.length / 4];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = ((bytes[4 * i] & 0xFF) << 24) |
                    ((bytes[4 * i + 1] & 0xFF) << 16) |
                    ((bytes[4 * i + 2] & 0xFF) << 8) |
                    ((bytes[4 * i + 3] & 0xFF) << 0);
        }

        // Wrap and return the int array.
        return new Tuple1<>(ints);
    }
}
