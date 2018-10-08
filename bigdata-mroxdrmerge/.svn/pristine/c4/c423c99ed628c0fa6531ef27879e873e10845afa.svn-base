package cn.mastercom.bigdata.mro.stat.register;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsFgTableEnum;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class MroTableRegister_CS_FG
{
	public String outpath_table;
	public String outpath_date;

	public MroTableRegister_CS_FG(String outpath_table, String dateStr)
	{
		this.outpath_table = outpath_table;
		this.outpath_date = dateStr.replace("01_", "");;
	}

	public void registerOutFileName(Job job)
	{
		for (MroCsFgTableEnum mroCsFgTableEnum : MroCsFgTableEnum.values())
		{
			MultiOutputMngV2.addNamedOutput(job, mroCsFgTableEnum.getIndex(), mroCsFgTableEnum.getFileName(), mroCsFgTableEnum.getPath(outpath_table, outpath_date), TextOutputFormat.class,
					NullWritable.class, Text.class);
		}

	}

}
