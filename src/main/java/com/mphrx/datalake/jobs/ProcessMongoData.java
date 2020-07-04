package com.mphrx.datalake.jobs;


import com.mphrx.datalake.config.DatalakeConfig;
import com.mphrx.datalake.services.CleanPatientDataService;
import com.mphrx.datalake.services.CleanUserDataService;
import io.delta.tables.DeltaTable;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class ProcessMongoData {
    protected static final Logger log =  LoggerFactory.getLogger(ProcessMongoData.class);
    @Autowired
    DatalakeConfig datalakeConfig;
    @Autowired
    CleanPatientDataService cleanPatientDataService;
    @Autowired
    CleanUserDataService cleanUserDataService;

    public void getRawDataFromMongo(String[] args){
        log.warn(">>>>>>getRawDataFromMongo Started To Fetch Mongo Raw With Arguments===>>"+ Arrays.toString(args));
        if(args.length > 1){
            SparkSession sparkMongoSession = datalakeConfig.SparkMongoSession();
            try {
                String collectionName = args[1];
                String fileStoragePath = datalakeConfig.getWriteFolder() + datalakeConfig.getRawDataPrefix() + collectionName;

                //Fetching Data From Mongo DB
                JavaSparkContext javaSparkContext = new JavaSparkContext(sparkMongoSession.sparkContext());
                Dataset<Row> mongoRead = sparkMongoSession.read().format("mongo").option("mode", "PERMISSIVE")
                        .option("columnNameOfCorruptRecord", "_corrupt_record").option("collection", collectionName).load();

                log.warn(">>>>>>>Total Fetched Rows Count From Collection "+collectionName+"=>"+mongoRead.count());
                mongoRead.printSchema();

                if(mongoRead.count() > 0) {
                    //Setting Partition format in date using lastUpdatedDate
                    mongoRead = mongoRead.withColumn("partitionDate", mongoRead.col("lastUpdated").cast(DataTypes.DateType));

                    //Writing Data In Delta Format On File Storage Or Object Storage
                    if(!datalakeConfig.getObjectStorage()){
                        mongoRead.write().format(datalakeConfig.getReadWriteFormat()).mode(SaveMode.Append)
                                .option("mergeSchema", "true").partitionBy("partitionDate").save(fileStoragePath);
                    }else {
                        //TODO
                    }
                }
            }catch (Exception ex){
                log.error("Exception occured during Mongo Raw Data processing"+ex.getMessage(), ex);
                throw ex;
            }finally {
                sparkMongoSession.stop();
            }

        }else{
            log.error(">>>>>>>>>getRawDataFromMongo : Please add valid arguments for collection name. Not Able to process futher.");
        }
    }

    public void getCleanDataInParquet(String[] args) {
        log.warn(">>>>>>getCleanDataInParquet Started To Clean Mongo Raw With Arguments===>>" + Arrays.toString(args));

        if (args.length > 1) {
            SparkSession sparkSession = datalakeConfig.SparkSession();
            String collectionName = args[1];
            String readFilePath = datalakeConfig.getWriteFolder() + datalakeConfig.getRawDataPrefix() + collectionName;
            String writeFilePath = datalakeConfig.getWriteFolder() + datalakeConfig.getCleanDataPrefix() + collectionName;

            try {
                //Calling function as per argument received and dataset defined
                Dataset<Row> dataset = null;
                if(collectionName.equals("patient")){
                    cleanPatientDataService.registerPateintUdf(sparkSession);
                    dataset = cleanPatientDataService.cleanPatientData(sparkSession, readFilePath);
                }else if(collectionName.equals("user")){
                    cleanUserDataService.registerUserUdf(sparkSession);
                    dataset= cleanUserDataService.cleanUserData(sparkSession, readFilePath);
                }else {
                    log.error("Colletion name not matched with defined clean up process ==>"+collectionName);
                }

                //Writing Data At Clean Storage
                if(dataset != null  && dataset.count() > 0) {
                    if (DeltaTable.isDeltaTable(writeFilePath)) {
                        DeltaTable.forPath(sparkSession, writeFilePath)
                                .as("destDataset")
                                .merge(dataset.toDF().as("sourceDataset"),
                                        "sourceDataset.mongoId = destDataset.mongoId"
                                ).whenMatched("sourceDataset.partitionDate = destDataset.partitionDate")
                                .updateAll()
                                .whenNotMatched()
                                .insertAll()
                                .execute();
                    } else {
                        log.warn("===========Delta Table witH record NOT exists========.Going to do the INSERT operation.");
                        dataset.write().format(datalakeConfig.getReadWriteFormat()).option("mergeSchema", "true").partitionBy("partitionDate").save(writeFilePath);
                    }
                }else {
                    log.warn("No Record found for collection "+collectionName);
                }
        }catch(Exception ex){
            log.error("Exception during getCleanDataInParquet=="+ex.getMessage(),ex);
        }finally {
             sparkSession.stop();
        }

    }else {
        log.error(">>>>>>>>>getCleanDataInParquet : Please add valid arguments for collection name. Not Able to process futher.");
        }
    }
}
