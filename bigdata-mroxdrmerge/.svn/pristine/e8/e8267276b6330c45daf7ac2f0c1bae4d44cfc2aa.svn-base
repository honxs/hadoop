package cn.mastercom.bigdata.spark.mroxdrmerge;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MroXdrMergeMain
{
	protected static final Log LOG = LogFactory.getLog(MroXdrMergeMain.class);

	public static void main(String[] args) throws Exception
	{
		try
		{
			MroXdrMergeJob job = new MroXdrMergeJob();
			long startMillis = System.currentTimeMillis();
			job.DoSparkJob(args);
			long endMillis = System.currentTimeMillis();
			System.out.println("Spend time: " + (endMillis - startMillis));
		}
		catch (Exception e)
		{
			System.out.println("Job Stop! : ");
			e.printStackTrace();
		}	
//		System.exit(0);
	}


}
