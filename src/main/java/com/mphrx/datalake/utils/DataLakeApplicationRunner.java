package com.mphrx.datalake.utils;

import com.mphrx.datalake.jobs.PrimaryDataProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(2)
public class DataLakeApplicationRunner implements ApplicationRunner {
    protected static final Logger log =  LoggerFactory.getLogger(DataLakeApplicationRunner.class);
    @Autowired
    private PrimaryDataProcessing primaryDataProcessing;
    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("ApplicationRunner started for data processing using spark deltlake");
       // primaryDataProcessing.getProcessingDetails();
        log.info("ApplicationRunner ended");
    }
}
