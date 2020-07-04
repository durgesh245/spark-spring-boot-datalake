package com.mphrx.datalake.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.context.annotation.Bean;


@ConfigurationProperties(prefix = "datalake")
@ConstructorBinding
public class DatalakeConfig {

    private final String readFolder;
    private final String writeFolder;
    private final String readWriteFormat;
    private final String rawDataPrefix;
    private final String cleanDataPrefix;
    private final Boolean isObjectStorage;
    private final SparkExternalConfig spark;


    public DatalakeConfig(String readFolder, String writeFolder, String readWriteFormat, String rawDataPrefix,
                          String cleanDataPrefix, Boolean isObjectStorage, SparkExternalConfig spark) {
        this.readFolder = readFolder;
        this.writeFolder = writeFolder;
        this.readWriteFormat = readWriteFormat;
        this.rawDataPrefix = rawDataPrefix;
        this.cleanDataPrefix = cleanDataPrefix;
        this.isObjectStorage = isObjectStorage;
        this.spark = spark;
    }

    public String getReadFolder() {
        return readFolder;
    }

    public String getWriteFolder() {
        return writeFolder;
    }

    public SparkExternalConfig getSpark() {
        return spark;
    }
    public String getReadWriteFormat() {
        return readWriteFormat;
    }

    public String getRawDataPrefix() {
        return rawDataPrefix;
    }

    public String getCleanDataPrefix() {
        return cleanDataPrefix;
    }

    public Boolean getObjectStorage() {
        return isObjectStorage;
    }

    public static class SparkExternalConfig{

        private final String url;
        private final String defaultAppName;
        private final String warehouseDir;
        private final String mongoInputUri;
        private final String mongoOutputUri;
        private final Double cleanFileOlderThan;

        public SparkExternalConfig(String url, String defaultAppName, String warehouseDir, String mongoInputUri, String mongoOutputUri, Double cleanFileOlderThan) {
            this.url = url;
            this.defaultAppName = defaultAppName;
            this.warehouseDir = warehouseDir;
            this.mongoInputUri = mongoInputUri;
            this.mongoOutputUri = mongoOutputUri;
            this.cleanFileOlderThan = cleanFileOlderThan;
        }
        public String getUrl() {
            return url;
        }

        public String getDefaultAppName() {
            return defaultAppName;
        }

        public String getWarehouseDir() {
            return warehouseDir;
        }

        public String getMongoInputUri() {
            return mongoInputUri;
        }

        public String getMongoOutputUri() {
            return mongoOutputUri;
        }
        public Double getCleanFileOlderThan() {
            return cleanFileOlderThan;
        }

    }


    public SparkSession SparkSession(){
       return SparkSession
                .builder()
               .config("spark.sql.warehouse.dir",spark.getWarehouseDir())
                .appName(spark.getDefaultAppName())
                .master(spark.getUrl())
               .config("spark.databricks.delta.retentionDurationCheck.enabled", false)
               .config("spark.databricks.delta.schema.autoMerge.enabled", true)
                .getOrCreate();
    }

    public SparkSession SparkMongoSession(){
        return  SparkSession
                .builder()
                .appName(spark.getDefaultAppName())
                .master(spark.getUrl())
                //.config("spark.mongodb.input.uri","mongodb://root:password@10.150.100.41:27018/uhgtest_minervadb?authSource=admin")
                .config("spark.mongodb.input.uri", spark.mongoInputUri)
                .config("spark.mongodb.output.uri", spark.mongoOutputUri)
                .config("spark.databricks.delta.retentionDurationCheck.enabled", false) //For delta lake vaccume
                .config("spark.databricks.delta.schema.autoMerge.enabled", true) // For delta lake Automatic schema evolution
                .getOrCreate();
    }

    public SparkSession SparkMinIoSession(){
        return SparkSession
                .builder()
                .config("spark.sql.warehouse.dir",spark.getWarehouseDir())
                .appName(spark.getDefaultAppName())
                .master(spark.getUrl())
                .config("spark.databricks.delta.retentionDurationCheck.enabled", false)
                .config("spark.databricks.delta.schema.autoMerge.enabled", true)
                .config("spark.hadoop.fs.s3a.endpoint","http://10.150.100.42:9000")
                .config("spark.hadoop.fs.s3a.access.key","minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key","minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access",true)
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();
    }
}

