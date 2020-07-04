package com.mphrx.datalake.controller;

import com.mphrx.datalake.jobs.CleanDataProcessing;
import com.mphrx.datalake.jobs.PrimaryDataProcessing;
import com.mphrx.datalake.jobs.ProcessAndLoadDataInMysql;
import com.mphrx.datalake.jobs.ProcessMongoData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JobRouterWithAgruments {
    protected static final Logger log =  LoggerFactory.getLogger(JobRouterWithAgruments.class);
    @Autowired
    PrimaryDataProcessing primaryDataProcessing;
    @Autowired
    CleanDataProcessing cleanDataProcessing;
    @Autowired
    ProcessMongoData processMongoData;
    @Autowired
    ProcessAndLoadDataInMysql processAndLoadDataInMysql;
    public void jobRouter(){

    }
    public void jobRouter(String[] args){
        log.warn("Action : ["+args[0]+"] received for job processing");
        switch (args[0]){
            case "processData" :  //used for testing
                primaryDataProcessing.getProcessingDetails(args);
                break;
            case "cleanFile" :   //used for testing
                primaryDataProcessing.cleanOldFiles(args);
                break;
            case "cleanData" :  //used for testing
                cleanDataProcessing.getProcessingDetails(args);
                break;
            case "processMongoRawData" :
                processMongoData.getRawDataFromMongo(args);
                break;
            case "cleanMongoRawData" :
                processMongoData.getCleanDataInParquet(args);
                break;
            case "loadDiscreteDataInMysql" :
                processAndLoadDataInMysql.loadDiscreteDataInMysql(args);
                break;
            case "loadDiscreteDataInMysqlFromRaw" :
                processAndLoadDataInMysql.loadDiscreteDataInMysqlFromRaw(args);
                break;
            case "loadAggregatedDataInMysql" :
                processAndLoadDataInMysql.loadAggregatedDataInMysql(args);
                break;
            default:
                log.warn("Action : ["+args[0]+"] not defined for job processing. Please use [processData/cleanData/]");
        }
    }
}
