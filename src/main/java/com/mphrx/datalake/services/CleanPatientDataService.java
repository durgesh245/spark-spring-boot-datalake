package com.mphrx.datalake.services;
import com.mphrx.datalake.config.DatalakeConfig;
import io.delta.tables.DeltaTable;
import org.apache.commons.lang3.StringUtils;
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

import java.util.ArrayList;

@Service
public class CleanPatientDataService {
    @Autowired
    DatalakeConfig datalakeConfig;
    protected static final Logger log =  LoggerFactory.getLogger(CleanPatientDataService.class);
    public Dataset cleanPatientData(SparkSession sparkSession, String readFilePath){
        log.warn("==========>>>>Clean Data processing. Getting Clean Patient data from cleanPatientData<<==============");
        Dataset<Row> dataset = null;
        //Reading data from received filePath
        if(DeltaTable.isDeltaTable(sparkSession, readFilePath)) {
            try {
                dataset = sparkSession.read().format(datalakeConfig.getReadWriteFormat()).load(readFilePath).
                        select(new Column("_id").as("mongoId"), functions.col("gender.value").as("gender"),
                                functions.callUDF("identifier", functions.col("identifier")).as("identifier"),
                                functions.col("dateCreated"), functions.col("active.value").as("active"),
                                functions.callUDF("dobConversion", functions.col("birthDate.value")).as("birthDate"),
                                functions.col("lowerCaseName.text.value").as("patientName"), functions.col("maritalStatus.text.value").as("maritalStatus")
                                , functions.col("lastUpdated"));

                //Setting Partition format in date using dateCreated
                dataset = dataset.withColumn("partitionDate", dataset.col("dateCreated").cast(DataTypes.DateType));
            }catch (Exception ex){
                log.error("Exception occures during patient data read from Raw format"+ex.getMessage(), ex);
                throw ex;
            }
            dataset.printSchema();
        }
        return dataset;
    }

    public void registerPateintUdf(SparkSession sparkSession){
        log.warn("==========>>>>Clean Patient Data processing started with registerPateintUdf<<==============");

        /*
        Defining user defined function to concatenate the identifiers
         */
        UserDefinedFunction identifierUdf = functions.udf((WrappedArray<GenericRowWithSchema> identifier) -> {
            ArrayList<String> identifierList = new ArrayList();
            if(identifier != null) {
                JavaConverters.asJavaCollectionConverter(identifier).asJavaCollection().forEach(ide -> {
                    String ideStr = "";
                    String sys = "";
                    String type = "";
                    String value = "";
                    String[] system = ide.schema().names();
                    for (String s : system) {
                        if (s.equals("system")) {
                            sys = ((Row) ide.getAs(s)).getString(0);
                        }
                        if (s.equals("type")) {
                            type = ((Row) ide.getAs(s)).getStruct(1).getString(0);
                        }
                        if (s.equals("value")) {
                            value = ((Row) ide.getAs(s)).getString(0);
                        }
                    }
                    if (sys != "" && type != "" && value != "") {
                        ideStr = sys + "_" + type + "_" + value;
                    } else if (value != "") {
                        ideStr = value;
                    }
                    if (ideStr != "")
                        identifierList.add(ideStr);
                });
            }
            return String.join(",", identifierList);
        }, DataTypes.StringType);

        /*
        Udf for DateType conversion (birthDate)
         */
        UserDefinedFunction convertToDate = functions.udf((java.sql.Timestamp dob) -> {
            if(dob != null)
                return new java.sql.Date(dob.getTime());
            else
                return null;
        }, DataTypes.DateType);

        sparkSession.udf().register("identifier", identifierUdf);
        sparkSession.udf().register("dobConversion", convertToDate);
    }
}
