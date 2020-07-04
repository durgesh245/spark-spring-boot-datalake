package com.mphrx.datalake.resources;

import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PatientResource {
    protected static final Logger log =  LoggerFactory.getLogger(PatientResource.class);

    public void getDetails(){
        log.info("All patient data is available");
    }

    public ArrayType getAddressStruc(){
        StructField[] address = new StructField[]{
            new StructField("city", DataTypes.createStructType(new StructField[]{
                    new StructField("value", DataTypes.StringType, true, Metadata.empty())}),
                    true, Metadata.empty()),
            new StructField("country", DataTypes.createStructType(new StructField[]{
                    new StructField("value", DataTypes.StringType, true, Metadata.empty())}),
                    true, Metadata.empty()),
                new StructField("line", ArrayType.apply(DataTypes.createStructType(new StructField[]{
                        new StructField("value", DataTypes.StringType, true, Metadata.empty())})),
                        true, Metadata.empty()),
                new StructField("postalCode", DataTypes.createStructType(new StructField[]{
                        new StructField("value", DataTypes.StringType, true, Metadata.empty())}),
                        true, Metadata.empty()),
                new StructField("state", DataTypes.createStructType(new StructField[]{
                        new StructField("value", DataTypes.StringType, true, Metadata.empty())}),
                        true, Metadata.empty()),
                new StructField("text", DataTypes.createStructType(new StructField[]{
                        new StructField("value", DataTypes.StringType, true, Metadata.empty())}),
                        true, Metadata.empty()),
                new StructField("useCode", DataTypes.createStructType(new StructField[]{
                        new StructField("value", DataTypes.StringType, true, Metadata.empty())}),
                        false, Metadata.empty())
        };
        return ArrayType.apply(DataTypes.createStructType(address), true);
    }
}
