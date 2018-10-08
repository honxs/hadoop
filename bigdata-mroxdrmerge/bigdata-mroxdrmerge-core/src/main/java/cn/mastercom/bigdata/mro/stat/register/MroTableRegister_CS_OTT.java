package cn.mastercom.bigdata.mro.stat.register;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class MroTableRegister_CS_OTT
{
	public String outpath_table;
	public String outpath_date;

	public MroTableRegister_CS_OTT(String outpath_table, String dateStr)
	{
		this.outpath_table = outpath_table;
		this.outpath_date = dateStr.replace("01_", "");;
	}

	public void registerOutFileName(Job job)
	{
		for (MroCsOTTTableEnum mroCsOTTTableEnum : MroCsOTTTableEnum.values())
		{
			MultiOutputMngV2.addNamedOutput(job, mroCsOTTTableEnum.getIndex(), mroCsOTTTableEnum.getFileName(), mroCsOTTTableEnum.getPath(outpath_table, outpath_date), TextOutputFormat.class,
					NullWritable.class, Text.class);

		}
	}
}
