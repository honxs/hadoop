package cn.mastercom.bigdata.mro.stat.register;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class MroTableRegister_BS
{
	public String outpath_table;
	public String outpath_date;

	public MroTableRegister_BS(String outpath_table, String dateStr)
	{
		this.outpath_table = outpath_table;
		this.outpath_date = dateStr.replace("01_", "");
	}

	public void registerOutFileName(Job job)
	{
		for (MroBsTablesEnum mroBsTablesEnum : MroBsTablesEnum.values())
		{
			MultiOutputMngV2.addNamedOutput(job, mroBsTablesEnum.getIndex(), mroBsTablesEnum.getFileName(), mroBsTablesEnum.getPath(outpath_table, outpath_date), TextOutputFormat.class,
					NullWritable.class, Text.class);
		}

	}

}
