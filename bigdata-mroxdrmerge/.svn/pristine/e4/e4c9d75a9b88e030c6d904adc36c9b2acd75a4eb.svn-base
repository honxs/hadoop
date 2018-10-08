package cn.mastercom.bigdata.locuser_v3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class CfgInfo
{
	public static ReportProgress rProgress = null;
	public static DistributedFileSystem hdfs = null;
	public static CfgCell cc = new CfgCell();
	public static CfgIndoor ci = new CfgIndoor();
	public static final Log LOG = LogFactory.getLog(CfgInfo.class);
	// public static CfgStat cs = new CfgStat();
	public CfgNCell cn = new CfgNCell();

	public boolean Init(ReportProgress rptProgress)
	{
		
		rProgress = rptProgress;
		if (!cc.Init(rptProgress))
		{
			return false;
		}
		if (!ci.Init(rptProgress))
		{
			return false;
		}
		// if (!cs.Init(rptProgress))
		// {
		// return false;
		// }
		if (!cn.Init(rptProgress))
		{
			return false;
		}
		return true;
	}

	public static BufferedReader getReader(String fname, ReportProgress rptProgress)
	{
		BufferedReader sr = null;
		Configuration conf = MainModel.GetInstance().getConf();
		try
		{
			boolean bZip = false;
			if (!fname.contains(":"))
			{
				if (hdfs == null)
				{
					try
					{
						hdfs = (DistributedFileSystem) FileSystem.get(conf);
					}
					catch (IOException e1)
					{
						hdfs = null;
						if (rptProgress != null)
							rptProgress.writeLog(0, "1@" + e1.getMessage());
						return null;
					}
				}
				Path fp = new Path(fname);
				if (!hdfs.exists(fp))
				{
					fname += ".gz";
					fp = new Path(fname);
					if (!hdfs.exists(fp))
					{
						if (rptProgress != null)
							rptProgress.writeLog(0, "not exist" + fname);
						return null;
					}
					bZip = true;
				}

				if (bZip)
				{
					sr = new BufferedReader(new InputStreamReader(new GZIPInputStream(hdfs.open(new Path(fname))), "UTF-8"));
				}
				else
				{
					sr = new BufferedReader(new InputStreamReader(hdfs.open(new Path(fname)), "UTF-8"));
				}
			}
			else
			{
				File fp = new File(fname);
				if (!fp.exists())
				{
					fname += ".gz";
					fp = new File(fname);
					if (!fp.exists())
					{
						return null;
					}
					bZip = true;
				}
				if (bZip)
				{
					sr = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fname)), "UTF-8"));
				}
				else
				{
					sr = new BufferedReader(new InputStreamReader(new FileInputStream(fname), "UTF-8"));
				}
			}
		}
		catch (Exception ee)
		{
			if (rptProgress != null)
				rptProgress.writeLog(0, "2@" + ee.getMessage());

			return null;
		}

		return sr;
	}
}

