package com.huawei.bigdata.mapreduce.local.lib;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class SecurityUtils
{
    /**
     * log process object
     */
    private static final Log LOG = LogFactory.getLog(SecurityUtils.class);
    private static String PRINCIPAL = "username.client.kerberos.principal";
    private static String KEYTAB = "username.client.keytab.file";

    /**
     * get conf object
     * 
     * @return Configuration
     */
    public static Configuration getConfiguration()
    {

        // Default load from conf directory
        Configuration conf = new Configuration();

        String userdir = System.getProperty("user.dir") + File.separator
                + "config_hw" + File.separator;

        conf.addResource(new Path(userdir + "yarn-site.xml"));
        conf.addResource(new Path(userdir + "mapred-site.xml"));
        conf.addResource(new Path(userdir + "core-site.xml"));
        conf.addResource(new Path(userdir + "hdfs-site.xml"));
        conf.addResource(new Path(userdir + "user-mapred.xml"));

        return conf;
    }

    /**
     * security login
     * 
     * @return boolean
     */
    public static Boolean login(Configuration conf) {
        boolean flag = false;

        try {
            // security mode
            if ("kerberos".equalsIgnoreCase(conf
                    .get("hadoop.security.authentication"))) {
                conf.set(PRINCIPAL, conf.get("username.client.kerberos.principal"));
                // keytab file
                conf.set(KEYTAB, System.getProperty("user.dir") + File.separator
                        + "config_hw" + File.separator + conf.get("username.client.keytab.file"));

    			// kerberos path
    			String krbfilepath = System.getProperty("user.dir")
    					+ File.separator + "conf" + File.separator + "krb5.conf";
    			System.setProperty("java.security.krb5.conf", krbfilepath);

    			flag = loginFromKeytab(conf);
    		}

        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;

    }
    
    
	public static Boolean loginFromKeytab(Configuration conf) {
		boolean flag = false;
		UserGroupInformation.setConfiguration(conf);

		try {
			UserGroupInformation.loginUserFromKeytab(conf.get(PRINCIPAL), conf.get(KEYTAB));
			LOG.info("Login successfully.");

			flag = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return flag;

	}
    
}
