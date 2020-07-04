package com.mphrx.datalake.jobs;
import com.mongodb.DBEncoder;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.sql.fieldTypes.Timestamp;
import com.mphrx.datalake.config.DatalakeConfig;
import com.mphrx.datalake.config.MysqlConf;
import com.mphrx.datalake.services.GenericUtilService;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.apache.spark.sql.types.DataType;
import com.mongodb.spark.MongoSpark;
import io.delta.tables.*;
import org.stringtemplate.v4.ST;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.*;

@Component
public class CleanDataProcessing {
    @Autowired
    GenericUtilService genericUtilService;
    @Autowired
    DatalakeConfig datalakeConfig;
    @Autowired
    MysqlConf mysqlConf;

    protected static final Logger log =  LoggerFactory.getLogger(CleanDataProcessing.class);
    public static final String IDENTIFIER_MASSAGE = "identifierMassage";
    public void getProcessingDetails(String[] args){
        SparkSession sparkSession = datalakeConfig.SparkSession();
        log.warn("==========>>>>Clean Data processing started<<=============sparkSession=>"+sparkSession);

        /*
        Defining user defined function
         */
        UserDefinedFunction identifierUdf = functions.udf((WrappedArray<GenericRowWithSchema> identifier) -> {
            ArrayList<String> identifierList = new ArrayList();
            JavaConverters.asJavaCollectionConverter(identifier).asJavaCollection().forEach(ide -> {
                String ideStr = "";
                String sys = "";
                String type = "";
                String value = "";
                String[] system = ide.schema().names();
                for (String s : system) {
                    if(s.equals("system")){
                        sys = ((Row)ide.getAs(s)).getString(0);
                    }
                    if(s.equals("type")){
                        type = ((Row)ide.getAs(s)).getStruct(1).getString(0);
                    }
                    if(s.equals("value")){
                        value = ((Row)ide.getAs(s)).getString(0);
                    }
                }
                if(sys != "" && type != "" && value != ""){
                    ideStr = sys+"_"+type+"_"+value;
                }else if(value != ""){
                    ideStr = value;
                }
                if(ideStr != "")
                 identifierList.add(ideStr);
            });
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

        /*
        Validate DateType from timestamp
         */
        UserDefinedFunction convertToTimestamp = functions.udf((java.sql.Timestamp ts) -> {
            if(ts != null) {
                String str = ts.toString();
                System.out.println("Time stamp received in formate of ==> "+str.length());
                if(str.length() != 23){
                    str = StringUtils.substring(str, 0, 20)+"136";
                    System.out.println("This is data going to change==>"+str);
                    return java.sql.Timestamp.valueOf(str);
                }else{
                    return ts;
                }
            }
            else
                return null;
        }, DataTypes.TimestampType);


        sparkSession.udf().register(IDENTIFIER_MASSAGE, identifierUdf);
        sparkSession.udf().register("dobConversion", convertToDate);
        sparkSession.udf().register("timestampValid", convertToTimestamp);

        //Cleaning patinet raw data and storing at different place
        if(DeltaTable.isDeltaTable(sparkSession, datalakeConfig.getWriteFolder()+"/patient")) {
            Dataset<Row> patientDf=  sparkSession.read().format(datalakeConfig.getReadWriteFormat()).load(datalakeConfig.getWriteFolder()+"/patient");

/*            Dataset<Row> newDs = patientDf.select(new Column("_id"), functions.col("gender.value").as("gender"),
                    functions.callUDF(IDENTIFIER_MASSAGE,functions.col("identifier")).as("identifier"),
                    functions.col("extension"), functions.col("dateCreated"), functions.col("active.value").as("active"), functions.col("lastUpdated"),
                    functions.col("lowerCaseName.text.value").as("patientName"), functions.col("birthDate.value").as("birthDate"),functions.col("maritalStatus.text.value").as("maritalStatus"),
                    functions.col("address"));*/

            //removing some column to make it compatible with
            Dataset<Row> newDs = patientDf.toDF().select(new Column("_id").as("mongoId"), functions.col("gender.value").as("gender"),
                    functions.callUDF(IDENTIFIER_MASSAGE,functions.col("identifier")).as("identifier"),
                    functions.col("dateCreated"), functions.col("active.value").as("active"),
                    functions.callUDF("dobConversion",functions.col("birthDate.value")).as("birthDate"),
                    functions.col("lowerCaseName.text.value").as("patientName"),functions.col("maritalStatus.text.value").as("maritalStatus")
                    ,functions.col("lastUpdated")
                    );


            newDs.show(false);
            newDs.printSchema();




            /*
            Inserting data into MySql
             */


/*            String url = "jdbc:mysql://127.0.0.1:3306/datalake";
            String tableName = "patient";
            Properties prop = new Properties();
            prop.setProperty("driver", "com.mysql.jdbc.Driver");
            prop.setProperty("user", "root");
            prop.setProperty("password", "admin");*/


            newDs.toDF().write().mode(SaveMode.Overwrite).jdbc(mysqlConf.getUrl(),"patient", mysqlConf.getMysqlProperties());
            //newDs.toDF().write().mode(SaveMode.Overwrite).jdbc(url,"patient", prop);
            System.out.println("========>>Data ingested successfully in Mysql ");
            sparkSession.stop();
        }
    }
}
