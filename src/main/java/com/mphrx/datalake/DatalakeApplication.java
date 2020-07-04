package com.mphrx.datalake;

import com.mphrx.datalake.config.DatalakeConfig;
import com.mphrx.datalake.config.MysqlConf;
import com.mphrx.datalake.controller.JobRouterWithAgruments;
import com.mphrx.datalake.jobs.PrimaryDataProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.log4j.*;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.apache.commons.lang3.ArrayUtils;

@SpringBootApplication(exclude = MongoAutoConfiguration.class)
@EnableConfigurationProperties({DatalakeConfig.class, MysqlConf.class})
public class DatalakeApplication {
	protected static final Logger log =  Logger.getRootLogger();
	public static void main(String[] args) {
		log.warn("===========MphRx Datalake application started========");
		//Here we can register or override environment valiables.
		if(args.length > 0 && args[0].contains("properties-file") && args[0].split("=").length == 2){
			String externalPropertyLocation=args[0].split("=")[1];
			log.warn("External property file location is =>"+externalPropertyLocation);
			System.setProperty("spring.config.location", externalPropertyLocation);
			args = ArrayUtils.remove(args, 0);
		}
		ConfigurableApplicationContext context = SpringApplication.run(DatalakeApplication.class, args);
		log.warn("===========Staring the data ingestion========");
		JobRouterWithAgruments jobRouterWithAgruments = context.getBean(JobRouterWithAgruments.class);
		if(args.length > 0) {
			jobRouterWithAgruments.jobRouter(args);
		}else {
			//jobRouterWithAgruments.jobRouter(new String[]{"processData"});
			log.error("Not able to process further. Please send the action as argument");
		}
		log.warn("===========MphRx Datalake application ended========");
	}
}
