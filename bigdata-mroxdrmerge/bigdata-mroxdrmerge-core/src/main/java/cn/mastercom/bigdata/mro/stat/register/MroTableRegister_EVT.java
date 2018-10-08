package cn.mastercom.bigdata.mro.stat.register;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class MroTableRegister_EVT
{
	public String outpath_table;
	public String outpath_date;

	public MroTableRegister_EVT(String outpath_table, String dateStr)
	{
		this.outpath_table = outpath_table;
		this.outpath_date = dateStr;
	}

	public void registerOutFileName(Job job)
	{
		for(TypeIoEvtEnum typeSampleEvtIO : TypeIoEvtEnum.values())
		{
			String curOutPath = "";
			curOutPath = typeSampleEvtIO.getPath(outpath_table, "01_" + outpath_date);
			MultiOutputMngV2.addNamedOutput(job, typeSampleEvtIO.getIndex(), "mro"+typeSampleEvtIO.getFileName(),
					curOutPath, TextOutputFormat.class, NullWritable.class, Text.class);
		}
	}

}
