package com.mphrx.datalake.jobs;
import com.mphrx.datalake.config.DatalakeConfig;
import com.mphrx.datalake.resources.PatientResource;
import com.mphrx.datalake.services.GenericUtilService;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.delta.tables.*;

import javax.xml.crypto.Data;


@Component
public class PrimaryDataProcessing {
    @Autowired
    GenericUtilService genericUtilService;
    //@Autowired
    //SparkSession sparkSession;
    @Autowired
    DatalakeConfig datalakeConfig;

    //@Autowired
    //SparkSession sparkMongoSession;
    @Autowired
    PatientResource patientResource;

    protected static final Logger log =  LoggerFactory.getLogger(PrimaryDataProcessing.class);
    public void getProcessingDetails(String[] args){
        if(!datalakeConfig.getObjectStorage()) {
            SparkSession sparkMongoSession = datalakeConfig.SparkMongoSession();
            log.warn("Data processing started");
            //String logFile = datalakeConfig.getReadFolder()+"/patientExport.json"; // Should be some file on your system


            // SparkSession spark = SparkSession.builder().appName("Patient Json Read").master("local").getOrCreate();
            //Dataset<Row> patient = sparkSession.read().json(logFile);
            //patient.printSchema();

            //patient.createOrReplaceTempView("patient");

            //Dataset<Row> versionDf = spark.sql("SELECT lastUpdated FROM patient");
            //versionDf.show(false);

            //Going to save data as parquet format
            //patient.select("name", "birthDate", "gender").write().format(datalakeConfig.getReadWriteFormat()).save(datalakeConfig.getWriteFolder()+"/patient");


            //patient.write().format(datalakeConfig.getReadWriteFormat()).save(datalakeConfig.getWriteFolder()+"/patient");

            //Dataset<Row> dfSql=  sparkSession.read().format(datalakeConfig.getReadWriteFormat()).load(datalakeConfig.getWriteFolder()+"/patient");
            //dfSql.createOrReplaceTempView("patient");
            //Dataset<Row> data = sparkSession.sql("select maritalStatus from parquet.`"+datalakeConfig.getWriteFolder()+"/patient`");
            //data.show(false);





        /*
            reading data from MongodDB Directly
         */

            log.warn("=======mongo connection context ===>" + sparkMongoSession);
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkMongoSession.sparkContext());
            Dataset<Row> mongoRead = sparkMongoSession.read().format("mongo").option("mode", "PERMISSIVE")
                    .option("columnNameOfCorruptRecord", "_corrupt_record").option("collection", "patient").load();

            //Get Selective data only from Mongo DB
            //1. Using normal In Query
           // Dataset<Row> mongoRead = sparkMongoSession.read().format("mongo").option("collection", "patient")
            //        .option("pipeline", "[{$match: {_id : {$in:[4]}}}]").load()
                    //.drop("address","extension")
                    ;

            //2. Using date range
            // Dataset<Row> mongoRead = sparkMongoSession.read().format("mongo").option("collection", "patient")
            //     .option("pipeline", "[{$match: {lastUpdated : {$gt: new ISODate('2020-03-12T00:00:00.000Z')," +
            //            "$lt: new ISODate('2020-03-13T23:23:59.999Z')},'isAnonymous.value': false}}, {$sort:{_id:1}}]").load();

            log.warn(">>>>>>>Total Rows Fetched Count ====" + mongoRead.count());
            mongoRead.printSchema();
            if (mongoRead.count() > 0) {

                //Adding new column in data set for partition
                mongoRead = mongoRead.withColumn("partitionDate", mongoRead.col("lastUpdated").cast(DataTypes.DateType));

                //Performing FieldMapping with POJO classes -> Not Working Properly specially in case of missing values which defined in class
                // mongoRead = mongoRead.withColumn("address1", mongoRead.col("address").cast(patientResource.getAddressStruc()));
                //trying with ctula pojo class
                mongoRead = mongoRead.withColumn("address1", mongoRead.col("address"));

                mongoRead.show(false);


                //mongoRead.write().format(datalakeConfig.getReadWriteFormat()).save(datalakeConfig.getWriteFolder()+"/patient");


        /*
            Update and merge through delta libraries
         */

                if (DeltaTable.isDeltaTable(sparkMongoSession, datalakeConfig.getWriteFolder() + "/patientJson")) {
                    log.warn("===========Delta Table witH record exists========.Going to do the merge operation.");

        /*   DeltaTable.forPath(sparkMongoSession, datalakeConfig.getWriteFolder() + "/patient")
                    .as("patient")
                    .merge(mongoRead.toDF().as("sourcePat"),
                            "sourcePat._id = patient._id"
                    ).whenMatched("sourcePat.partitionDate = patient.partitionDate")
                    .updateAll()
                    .whenNotMatched()
                    .insertAll()
                    .execute();*/

                    //Append mode to check the data missmatch issues
                    mongoRead.write().format(datalakeConfig.getReadWriteFormat()).mode(SaveMode.Append).option("mergeSchema", "true").partitionBy("partitionDate").save(datalakeConfig.getWriteFolder() + "/patient");

                } else {
                    log.warn("===========Delta Table witH record NOT exists========.Going to do the INSERT operation.");
               //     mongoRead.write().format(datalakeConfig.getReadWriteFormat()).mode(SaveMode.Append).option("mergeSchema", "true").partitionBy("partitionDate").save(datalakeConfig.getWriteFolder() + "/patientJson");
                    mongoRead.write().mode(SaveMode.Append).option("mergeSchema", "true").partitionBy("partitionDate").json(datalakeConfig.getWriteFolder() + "/patientJson");

                }

                //log.warn("Is Delta Table ======>"+DeltaTable.isDeltaTable(datalakeConfig.getWriteFolder()+"/patient"));

                //Deleting not useful parquet files from the system.
                //DeltaTable.forPath(sparkMongoSession, datalakeConfig.getWriteFolder()+"/patient").vacuum(0.1);

                // Dataset<Row> data = sparkMongoSession.sql("select _id, birthDate, maritalStatus from parquet.`"+datalakeConfig.getWriteFolder()+"/patient`");
                //data.show(false);

            } else {
                log.warn(">>>>>>>No Data found With Selected Criteria<<<<<<<<<<<<<");
            }

            //Using delta lake

/*            Dataset<Row> dfSql = sparkMongoSession.read().format(datalakeConfig.getReadWriteFormat()).load(datalakeConfig.getWriteFolder() + "/patient");
            dfSql.createOrReplaceTempView("patient");
            Dataset<Row> data1 = sparkMongoSession.sql("select name from patient");
            data1.show(30, false);*/

            sparkMongoSession.stop();
            log.warn("Data processing completed");
        }else {

            log.warn("Started exploring data storage at MinIo (Object storage) using S3 apis");

            SparkSession sparkMinIoSession = datalakeConfig.SparkMinIoSession();
            //Dataset<Row> jsonDs = sparkMinIoSession.read().json("s3a://datalake/patientExport.json");
            Dataset<Row> jsonDs = sparkMinIoSession.read().parquet("s3a://datalake/patient.parqet");

            //Reading from local and writting to object storage
           // Dataset jsonDs = sparkMinIoSession.read().option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record")
            //        .option("dateFormat", "yyyy-MM-dd").option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
            //        .json(datalakeConfig.getWriteFolder() + "/patientJson/partitionDate=2020-03-20");
            jsonDs = jsonDs.withColumn("partitionDate", jsonDs.col("lastUpdated").cast(DataTypes.DateType));

            jsonDs.printSchema();
            jsonDs.show(false);

          //  jsonDs.write().format("parquet").mode(SaveMode.Append)
          //          .option("mergeSchema", "true").partitionBy("partitionDate").save( "s3a://datalake/patient.parqet");

            log.warn("Data write successfully in object storage ");
        }
    }

    public void cleanOldFiles(String[] args){
        log.warn("Going to clean the old data config--->"+datalakeConfig.getSpark().getCleanFileOlderThan());
        DeltaTable.forPath(datalakeConfig.SparkMongoSession(), datalakeConfig.getWriteFolder()+"/patient").vacuum(datalakeConfig.getSpark().getCleanFileOlderThan());
        log.warn("Data Cleaned Older Then ==> "+datalakeConfig.getSpark().getCleanFileOlderThan());
    }
}
