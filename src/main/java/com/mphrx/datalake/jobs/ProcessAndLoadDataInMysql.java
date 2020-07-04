package com.mphrx.datalake.jobs;

import com.mphrx.datalake.config.DatalakeConfig;
import com.mphrx.datalake.config.MysqlConf;
import com.mphrx.datalake.services.CleanPatientDataService;
import com.mphrx.datalake.services.CleanUserDataService;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class ProcessAndLoadDataInMysql {
    protected static final Logger log =  LoggerFactory.getLogger(ProcessAndLoadDataInMysql.class);
    @Autowired
    DatalakeConfig datalakeConfig;
    @Autowired
    CleanUserDataService cleanUserDataService;
    @Autowired
    CleanPatientDataService cleanPatientDataService;
    @Autowired
    MysqlConf mysqlConf;

    public void loadDiscreteDataInMysql(String[] args){
        log.warn(">>>>>>loadDiscreteDataInMysql Started To Load clean data in Mysql With Arguments===>>" + Arrays.toString(args));

        if (args.length > 1) {
            SparkSession sparkSession = datalakeConfig.SparkSession();
            String collectionName = args[1];
            String readFilePath = datalakeConfig.getWriteFolder() + datalakeConfig.getCleanDataPrefix() + collectionName;

            try {
                //Calling function as per argument received and dataset defined.
                Dataset<Row> dataset = null;
                if(DeltaTable.isDeltaTable(sparkSession, readFilePath)) {
                    //Fetch data from clean parquet data set
                    dataset = sparkSession.read().format(datalakeConfig.getReadWriteFormat()).load(readFilePath);
                }
                //Writing Data At Mysql Storage
                if(dataset != null  && dataset.count() > 0) {
                    //Loading data into Mysql
                    dataset.toDF().write().mode(SaveMode.Overwrite).jdbc(mysqlConf.getUrl(),collectionName, mysqlConf.getMysqlProperties());

                    log.warn("========>>Data ingested successfully in Mysql for collection "+collectionName);
                }else {
                    log.warn("No Record found to Load in Mysql for collection "+collectionName);
                }
            }catch(Exception ex){
                log.error("Exception during getCleanDataInParquet=="+ex.getMessage(),ex);
            }finally {
                sparkSession.stop();
            }

        }else {
            log.error(">>>>>>>>>loadDiscreteDataInMysql : Please add valid arguments for collection name. Not Able to process futher.");
        }
    }

    /*
    Not Recommended as this contains the dumpicate records
     */
    public void loadDiscreteDataInMysqlFromRaw(String[] args){
        log.warn(">>>>>>loadDiscreteDataInMysqlFromRaw Started To Load Raw data in Mysql With Arguments===>>" + Arrays.toString(args));

        if (args.length > 1) {
            SparkSession sparkSession = datalakeConfig.SparkSession();
            String collectionName = args[1];
            String readFilePath = datalakeConfig.getWriteFolder() + datalakeConfig.getRawDataPrefix() + collectionName;

            try {
                //Calling function as per argument received and dataset defined.
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
                //Writing Data At Mysql Storage
                if(dataset != null  && dataset.count() > 0) {
                    //Loading data into Mysql
                    dataset.toDF().write().mode(SaveMode.Overwrite).jdbc(mysqlConf.getUrl(),collectionName, mysqlConf.getMysqlProperties());

                    log.warn("========>>Data ingested successfully in Mysql for collection "+collectionName);
                }else {
                    log.warn("No Record found to Load in Mysql for collection "+collectionName);
                }
            }catch(Exception ex){
                log.error("Exception during loadDiscreteDataInMysqlFromRaw=="+ex.getMessage(),ex);
            }finally {
                sparkSession.stop();
            }

        }else {
            log.error(">>>>>>>>>loadDiscreteDataInMysqlFromRaw : Please add valid arguments for collection name. Not Able to process futher.");
        }
    }

    /*
    This can only perform from clean data. We cannot process using Raw storage as data getting appended and join will not give
    desired result
     */
    public void loadAggregatedDataInMysql(String[] args){
        log.warn(">>>>>>loadAggregatedDataInMysql Started To Load clean data in Mysql With Arguments===>>" + Arrays.toString(args));

        if (args.length > 2) {
            SparkSession sparkSession = datalakeConfig.SparkSession();
            String primaryCollectionName = args[1];
            String secondaryCollectionName = args[2];

            String readFilePath1 = datalakeConfig.getWriteFolder() + datalakeConfig.getCleanDataPrefix() + primaryCollectionName;
            String readFilePath2 = datalakeConfig.getWriteFolder() + datalakeConfig.getCleanDataPrefix() + secondaryCollectionName;

            try {
                //Calling function as per argument received and dataset defined.
                Dataset<Row> dataset1 = null;
                Dataset<Row> dataset2 = null;
                Dataset<Row> datasetJoin = null;
                if(DeltaTable.isDeltaTable(sparkSession, readFilePath1) && DeltaTable.isDeltaTable(sparkSession, readFilePath2)) {
                    //Fetch data from clean parquet data set
                    log.warn("Going to start reading the both data to perform join operation");
                    dataset1 = sparkSession.read().format(datalakeConfig.getReadWriteFormat()).load(readFilePath1)
                            .withColumnRenamed("partitionDate", "updatedDate");
                    //dataset1.select("mongoId").show(false);
                    dataset2 = sparkSession.read().format(datalakeConfig.getReadWriteFormat()).load(readFilePath2)
                            .withColumnRenamed("mongoId", "userMongoId").withColumnRenamed("dateCreated", "userDateCreated")
                            .withColumnRenamed("lastUpdated", "userLastUpdated").withColumnRenamed("gender", "userGender")
                            .withColumnRenamed("partitionDate", "userPartitionDate");
                   // dataset2.select("patientId").show(false);
                    datasetJoin = dataset1.join(dataset2, dataset1.col("mongoId").equalTo(dataset2.col("patientid")),"inner");
                }
                //Writing Data At Mysql Storage
                if(datasetJoin != null  && datasetJoin.count() > 0 ) {
                    //Loading data into Mysql
                    datasetJoin.show(false);
                        datasetJoin.toDF().write().mode(SaveMode.Overwrite).jdbc(mysqlConf.getUrl(),
                                primaryCollectionName+"_"+secondaryCollectionName, mysqlConf.getMysqlProperties());


                    /*
                    Performing some operation on collected Data for User
                     */
                    dataset2 = dataset2.withColumn("lastLoginDate", dataset2.col("lastLoginTimeStamp").cast(DataTypes.DateType));
                    Dataset<Row> aggData = dataset2
                            //.where(dataset2.col("address").isNotNull())
                            .groupBy("enabled","lastLoginDate", "userGender", "userPartitionDate").count();
                    aggData.show(false);
                    aggData.toDF().write().mode(SaveMode.Overwrite).jdbc(mysqlConf.getUrl(),
                            primaryCollectionName+"_aggData", mysqlConf.getMysqlProperties());
                    log.warn("========>>Data ingested successfully in Mysql for collections =>"+primaryCollectionName+" and =>"+secondaryCollectionName);
                }else {
                    log.warn("No Record found to Load in Mysql for collections =>"+primaryCollectionName+ " and =>"+secondaryCollectionName);
                }
            }catch(Exception ex){
                log.error("Exception during loadAggregatedDataInMysql=="+ex.getMessage(),ex);
            }finally {
                sparkSession.stop();
            }

        }else {
            log.error(">>>>>>>>>loadAggregatedDataInMysql : Please add valid arguments for collection name. Not Able to process futher.");
        }
    }

}

