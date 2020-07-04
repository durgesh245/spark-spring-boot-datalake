package com.mphrx.datalake.services;
import com.mphrx.datalake.config.DatalakeConfig;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

@Service
public class CleanUserDataService {
    @Autowired
    DatalakeConfig datalakeConfig;
    protected static final Logger log =  LoggerFactory.getLogger(CleanUserDataService.class);

    public Dataset cleanUserData(SparkSession sparkSession, String readFilePath){
        log.warn("==========>>>>Clean Data processing. Getting Clean User data from cleanUserData<<==============");
        Dataset<Row> dataset = null;
        //Reading data from received filePath
        if(DeltaTable.isDeltaTable(sparkSession, readFilePath)) {
            try {
                dataset = sparkSession.read().format(datalakeConfig.getReadWriteFormat()).load(readFilePath)
                        .filter("userType == 'PATIENT'")
                        .select(new Column("_id").as("mongoId"), functions.col("email"), functions.col("enabled"),
                                functions.col("firstName"), functions.col("lastName"), functions.col("phoneNo"), functions.col("userType"),
                                functions.col("dateCreated"), functions.col("lastLoginTimeStamp"), functions.col("patientId"),
                                functions.callUDF("dobConversion", functions.col("dob")).as("dob"),
                                functions.col("gender"), functions.col("countryCode"), functions.callUDF("intArrayToString", functions.col("userGroups")).as("userGroups")
                                , functions.col("lastUpdated"));

                //Setting Partition format in date using dateCreated
                dataset = dataset.withColumn("partitionDate", dataset.col("dateCreated").cast(DataTypes.DateType));
                dataset.show(false);
            }catch (Exception ex){
                log.error("Exception occures during patient data read from Raw format"+ex.getMessage(), ex);
                throw ex;
            }
            dataset.printSchema();
        }
        return dataset;
    }

    public void registerUserUdf(SparkSession sparkSession){
        log.warn("==========>>>>Clean User Data processing started with registerUserUdf<<==============");
        /*
        Udf for DateType conversion (birthDate)
         */
        UserDefinedFunction convertToDate = functions.udf((java.sql.Timestamp dob) -> {
            if(dob != null)
                return new java.sql.Date(dob.getTime());
            else
                return null;
        }, DataTypes.DateType);

                /*
        Udf for Arrya[Integer] To String Conversion
         */
        UserDefinedFunction convertArrayToString = functions.udf((WrappedArray<Long> arrayVal) -> {
            if(arrayVal != null && arrayVal.length() >0 ) {
                List<String> value = new ArrayList();
                    JavaConverters.asJavaCollectionConverter(arrayVal).asJavaCollection().forEach(id -> {
                        value.add(id.toString());
                });
                return String.join(",", value);
            }
            else
                return "";
        }, DataTypes.StringType);

        sparkSession.udf().register("dobConversion", convertToDate);
        sparkSession.udf().register("intArrayToString", convertArrayToString);
    }
}
