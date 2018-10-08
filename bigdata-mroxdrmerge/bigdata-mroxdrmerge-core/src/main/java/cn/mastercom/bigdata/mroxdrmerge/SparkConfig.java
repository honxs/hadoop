package cn.mastercom.bigdata.mroxdrmerge;

import cn.mastercom.bigdata.util.IConfigure;

public class SparkConfig implements IConfigure
{
	private IConfigure conf;

	public SparkConfig(IConfigure conf)
	{
		this.conf = conf;

	}

	@Override
	public boolean loadConfigure()
	{
		return conf.loadConfigure();
	}

	@Override
	public boolean saveConfigure()
	{
		return conf.saveConfigure();
	}

	@Override
	public Object getValue(String name)
	{
		return conf.getValue(name);
	}

	@Override
	public Object getValue(String name, Object defaultValue)
	{
		return conf.getValue(name, defaultValue);
	}

	@Override
	public boolean setValue(String name, Object value)
	{
		return conf.setValue(name, value);
	}

	// spark.driver.maxResultSize
	public String get_spark_driver_maxResultSize()
	{
		return conf.getValue("spark_driver_maxResultSize", "8g").toString();
	}

	public boolean set_spark_driver_maxResultSize(String value)
	{
		return conf.setValue("spark_driver_maxResultSize", value);
	}
	
	//spark.master
	public String get_spark_master()
	{
		return conf.getValue("spark_master", "yarn").toString();
	}
	
	public boolean set_spark_master(String value)
	{
		return conf.setValue("spark_master", value);
	}
	
	//spark.deploy.mode
	public String get_deploy_mode()
	{
		return conf.getValue("spark_deploy_mode", "cluster").toString();
	}
	
	public boolean set_deploy_mode(String value)
	{
		return conf.setValue("spark_deploy_mode", value);
	}
	
	//yarn_nodemanager_resource_memory
	public String get_yarn_nodemanager_resource_memory()
	{
		return conf.getValue("yarn_nodemanager_resource_memory-mb", "8192").toString();
	}
	
	public boolean set_yarn_nodemanager_resource_memory(String value)
	{
		return conf.setValue("yarn_nodemanager_resource_memory-mb", value);
	}
	
	//yarn_nodemanager_resource_memory
	public String get_yarn_scheduler_maximum()
	{
		return conf.getValue("yarn_scheduler_maximum-allocation-mb", "8192").toString();
	}
	
	public boolean set_yarn_scheduler_maximum(String value)
	{
		return conf.setValue("yarn_scheduler_maximum-allocation-mb", value);
	}
	
	//hive database 
	public String get_HiveDBName()
	{
		return conf.getValue("HiveDBName", "default").toString();
	}
	
	public boolean set_HiveDBName(String value)
	{
		return conf.setValue("HiveDBName", value);
	}
	
	
}
