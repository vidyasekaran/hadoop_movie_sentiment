package com.dataflair.bd.proc.avg;
import org.apache.hadoop.conf.Configuration;

public class ConfigurationFactory {

private ConfigurationFactory() {

}

private final static Configuration conf = new Configuration();

public static Configuration getInstance() {
return conf;
}

}