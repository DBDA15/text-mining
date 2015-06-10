package de.hpi.fgis.dbda.textmining.MainTask_flink;

import java.io.Serializable;

public class SeedTuple implements Serializable {
    public String ORGANIZATION;
    public String LOCATION;

    public SeedTuple(String line) {
        String[] values = line.split("\t");
        ORGANIZATION = values[0];
        LOCATION = values[1];
    }
}