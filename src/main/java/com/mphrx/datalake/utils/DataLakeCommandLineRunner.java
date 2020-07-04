package com.mphrx.datalake.utils;

import com.mphrx.datalake.jobs.PrimaryDataProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(1)
public class DataLakeCommandLineRunner implements CommandLineRunner {
    protected static final Logger log =  LoggerFactory.getLogger(DataLakeCommandLineRunner.class);
    @Autowired
    private PrimaryDataProcessing primaryDataProcessing;
    @Override
    public void run(String... args) throws Exception {
        log.info("CMD started for data processing using spark deltlake");
        //primaryDataProcessing.getProcessingDetails();
        log.info("CMD ended");
    }
}
