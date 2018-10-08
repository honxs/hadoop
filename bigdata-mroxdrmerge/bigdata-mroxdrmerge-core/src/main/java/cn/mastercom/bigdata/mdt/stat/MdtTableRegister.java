package cn.mastercom.bigdata.mdt.stat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class MdtTableRegister
{
	public String outpath_table;
	public String outpath_date;

	public MdtTableRegister(String outpath_table, String dateStr)
	{
		this.outpath_table = outpath_table;
		this.outpath_date = dateStr.replace("01_", "");
	}

	public void registerOutFileName(Job job)
	{
		for (MdtTablesEnum mdtTablesEnum : MdtTablesEnum.values())
		{
			MultiOutputMngV2.addNamedOutput(job, mdtTablesEnum.getIndex(), mdtTablesEnum.getFileName(), mdtTablesEnum.getPath(outpath_table, outpath_date), TextOutputFormat.class, NullWritable.class,
					Text.class);

		}

	}

}
