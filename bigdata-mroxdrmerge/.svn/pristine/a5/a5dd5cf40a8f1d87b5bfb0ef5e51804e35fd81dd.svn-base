package cn.mastercom.bigdata.xdr.loc.register;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentLocTablesEnum;
import cn.mastercom.bigdata.util.hadoop.mapred.MultiOutputMngV2;

public class XdrTableRegister
{
	public String outpath_table;
	public String outpath_date;

	public XdrTableRegister(String outpath_table, String dateStr)
	{
		this.outpath_table = outpath_table;
		this.outpath_date = dateStr.replace("01_", "");//去掉01_
	}

	public void registerOutFileName(Job job)
	{
		for (ResidentLocTablesEnum residentLocTablesEnum : ResidentLocTablesEnum.values())
		{
			MultiOutputMngV2.addNamedOutput(job, residentLocTablesEnum.getIndex(), residentLocTablesEnum.getFileName(), residentLocTablesEnum.getPath(outpath_table, outpath_date),
					TextOutputFormat.class, NullWritable.class, Text.class);
		}
		for (XdrLocTablesEnum xdrTableEnum : XdrLocTablesEnum.values())
		{
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.NoCsTable))
			{
				
				 if (XdrLocTablesEnum.xdrLocation == xdrTableEnum)
				{
					MultiOutputMngV2.addNamedOutput(job, xdrTableEnum.getIndex(), xdrTableEnum.getFileName(), xdrTableEnum.getPath(outpath_table, outpath_date), TextOutputFormat.class, NullWritable.class,
							Text.class);
				}
				 if (XdrLocTablesEnum.xdruserinfo == xdrTableEnum)
				{
					MultiOutputMngV2.addNamedOutput(job, xdrTableEnum.getIndex(), xdrTableEnum.getFileName(), xdrTableEnum.getPath(outpath_table, outpath_date), TextOutputFormat.class, NullWritable.class, 
							Text.class);
				}
			}
			else
			{
				if (XdrLocTablesEnum.xdrLocSpan == xdrTableEnum)
				{
					continue;
				}
				
				else if (XdrLocTablesEnum.xdruseract == xdrTableEnum)
				{
					MultiOutputMngV2.addNamedOutput(job, xdrTableEnum.getIndex(), xdrTableEnum.getFileName(), xdrTableEnum.getPath(outpath_table, outpath_date), TextOutputFormat.class,
							NullWritable.class, Text.class);
				}
				else
				{
					MultiOutputMngV2.addNamedOutput(job, xdrTableEnum.getIndex(), xdrTableEnum.getFileName(), xdrTableEnum.getPath(outpath_table, outpath_date), TextOutputFormat.class, NullWritable.class,
							Text.class);
				}
			}
		}
	}

}
