package cn.mastercom.bigdata.mapr.stat.userAna;

import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import cn.mastercom.bigdata.stat.userAna.tableEnums.HsrEnums;
import cn.mastercom.bigdata.stat.userAna.tableEnums.SubwayEnums;

public class UserAnaAllMain
{

	public static void main(String[] args) throws Exception{
		
		//queue/date/input/output
//		args = new String[]{"NULL", "01_171120", "E:/文件/高铁测试配置/test/xdr_location", "E:/文件/高铁测试配置/test/out"};
		run(args);
	}
	
	public static boolean run(String[] args) throws Exception{

		Job job1 = PotentialUserAnaMain.CreateJob(args);
		Job job2 = UserAnaMain.CreateJob(args);

		//tmp
		return doJob(job1, args, true) && doJob(job2, args, false);
		
		/*boolean outResult1 =  doJob(job1, args, true) && doJob(job2, args, true) ;
		String date = args[1].trim().replace("01_", "");
		String outpath = args[3];
		args[2]=HsrEnums.HSR_XDR_INFO.getPath(outpath, date);
		Job job3 = UserAreaAnaMain.CreateJob(args);
		return outResult1 && doJob(job3, args, false);*/
	}
	
	private static boolean doJob(Job job, String[] args, boolean delOutputDir){
		boolean succeed = false;
		try
		{
			FileSystem fs = FileSystem.get(job.getConfiguration());
			
			Path outPath = null;
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2)){			
				outPath = new Path(HsrEnums.getBasePath(args[3], args[1].replace("01_", ""))+"/output");
			}
			else if(MainModel.GetInstance().getCompile().Assert(CompileMark.Subway)){
				outPath = new Path(SubwayEnums.getBasePath(args[3], args[1].replace("01_", ""))+"/output");
			}
			//提交job前删除output目录
			fs.delete(outPath, true);
			succeed = job.waitForCompletion(true);
			
			//job运行完删除output目录
			if(delOutputDir && fs.exists(outPath))
				fs.delete(outPath, true);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			succeed = false;
		}
		return succeed;
	}
}
