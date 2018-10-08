package cn.mastercom.bigdata.mroxdrmerge;

import cn.mastercom.bigdata.util.*;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import com.chinamobile.xdr.LocationParseService;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.InputStream;

public class MainModel
{
	public static void main(String[] args)
	{
		try{
			ParseItem parseItem = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-S1-HTTP");
			DataAdapterReader dataAdapterReader =	new DataAdapterReader(parseItem);
			
			ParseItem parseItem1 = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-Mw");
			DataAdapterReader dataAdapterReader1 =	new DataAdapterReader(parseItem1);
			
			ParseItem parseItem2 = MainModel.GetInstance().getEventAdapterConfig().getParseItem("LOCALL-Rx");
			DataAdapterReader dataAdapterReader2 =	new DataAdapterReader(parseItem2);
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	
	protected static Logger LOG = Logger.getLogger(MainModel.class);
	public boolean changedMmePosFlag = false;// 改变了mme 的位置信息

	private static MainModel instance = null;

	public static MainModel GetInstance()
	{
		if (instance == null)
		{
			LOG.info("create MainModel instance.");

			instance = new MainModel();

			LOG.info("loading config...");
			// 加载程序配置
			if (!instance.loadConfig())
			{
				LOG.info("load config error!");
				return null;
			}

			LOG.info("loading config success!");
		}
		return instance;
	}

	private MainModel()
	{
		File file = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
		if (file.isDirectory())
		{
			exePath = file.getPath();
		}
		else
		{
			exePath = file.getParent();
		}
	}

	private final String exePath;

	public String getExePath()
	{
		return exePath;
	}

	private AppConfig appConfig;

	public AppConfig getAppConfig()
	{
		return appConfig;
	}
	
	private SparkConfig sparkConfig;

	public SparkConfig getSparkConfig()
	{
		return sparkConfig;
	}

	private DataAdapterConf dataAdapterConfig;

	public DataAdapterConf getDataAdapterConfig()
	{
		return dataAdapterConfig;
	}

	private DataAdapterConf eventAdapterConfig;

	public DataAdapterConf getEventAdapterConfig()
	{
		return eventAdapterConfig;
	}

	private CompileHelper compile;

	public CompileHelper getCompile()
	{
		return compile;
	}

	private Configuration conf;

	public Configuration getConf()
	{
		return conf;
	}

	public void setConf(Configuration conf)
	{
		this.conf = conf;
	}
	private Configuration hbaseConf;
    public Configuration getHbaseConf()
    {
    	return hbaseConf;
    }
    public void setHbaseConf(Configuration hbaseConf)
    {
    	this.hbaseConf = hbaseConf;
    }
//	public String getFsUrl()
//	{
//		return conf.get("fs.defaultFS");
//	}

	public boolean loadConfig()
	{
		LOG.info("exe start up path: " + exePath);
		LOG.info("ClassLoader: " + getClass().getClassLoader().toString());

		// 读取开关文件
		try
		{
			if (new File(getExePath() + "/config/compile.conf").exists())
			{
				LOG.info("read compile file： " + getExePath() + "/config/compile.conf");

				CompileCfgMng.getInstance().init_txt(getExePath() + "/config/compile.conf");
				compile = CompileCfgMng.getInstance().getCompile("default");
			}
			else
			{
				LOG.info("loading compile.conf... ");

				InputStream insCompileConfig = getClass().getClassLoader().getResourceAsStream("compile.conf");

				if (insCompileConfig == null)
				{
					return false;
				}

				try
				{
					CompileCfgMng.getInstance().init(insCompileConfig);
					compile = CompileCfgMng.getInstance().getCompile("default");
				}
				catch (Exception e)
				{
					throw e;
				}
				finally
				{
					try
					{
						insCompileConfig.close();
					}
					catch (Exception e2)
					{
						// TODO: handle exception
					}

				}

				LOG.info("load compile.conf successed! ");
			}
		}
		catch (Exception e)
		{
			LOG.info("load compile.conf error: " + e.getMessage());
			return false;
		}

		// 读取基础配置
		String path_app = "app.xml";
		String path_adapter = "xdr_data_adapter.conf";
		String path_event_adapter = "event_data_adapter.conf";
		String path_spark = "spark_Home.xml";
		if (getCompile().Assert(CompileMark.GuiZhou))
		{
			LOG.info("read GuiZhou config... ");

			path_app = "app_GuiZhou.xml";
			path_adapter = "xdr_data_adapter_GuiZhou.conf";
		}
		else if (getCompile().Assert(CompileMark.HaErBin))
		{
			LOG.info("read HaErBin config... ");

			path_app = "app_HaErBin.xml";
			path_adapter = "xdr_data_adapter_HaErBin.conf";
			path_event_adapter = "event_data_adapter_HaErBin.conf";
		}
		else if (getCompile().Assert(CompileMark.LiaoNing))
		{
			LOG.info("read LiaoNing config... ");

			path_app = "app_LiaoNing.xml";
			path_adapter = "xdr_data_adapter_LiaoNing.conf";
		}
		else if (getCompile().Assert(CompileMark.ShangHai))
		{
			LOG.info("read ShangHai config... ");

			path_app = "app_ShangHai.xml";
			path_adapter = "xdr_data_adapter_ShangHai.conf";
			path_event_adapter = "event_data_adapter_ShangHai.conf";
		}

		else if (getCompile().Assert(CompileMark.GanSu))
		{
			LOG.info("read GanSu config... ");

			path_app = "app_GanSu.xml";
			path_adapter = "xdr_data_adapter_GanSu.conf";
			path_event_adapter = "event_data_adapter_GanSu.conf";
		}
		else if (getCompile().Assert(CompileMark.ShenZhen))
		{
			LOG.info("read ShenZhen config... ");

			path_app = "app_ShenZhen.xml";
			path_adapter = "xdr_data_adapter_ShenZhen.conf";
		}
		
		else if (getCompile().Assert(CompileMark.GuangXi2))
		{
			LOG.info("read GuangXi2 config... ");

			path_app = "app_GuangXi2.xml";
			path_adapter = "xdr_data_adapter_GuangXi2.conf";
			path_event_adapter = "event_data_adapter_GuangXi2.conf";
		}

		else if (getCompile().Assert(CompileMark.ShanXi))
		{
			LOG.info("read ShanXi config... ");

			path_app = "app_ShanXi.xml";
			path_adapter = "xdr_data_adapter_ShanXi.conf";
			path_event_adapter = "event_data_adapter_ShanXi.conf";
		}
		else if (getCompile().Assert(CompileMark.SiChuan))
		{
			LOG.info("read SiChuan config... ");

			path_app = "app_SiChuan.xml";
			path_adapter = "xdr_data_adapter_SiChuan.conf";
			path_event_adapter = "event_data_adapter_SiChuan.conf";
		}
		else if (getCompile().Assert(CompileMark.NingXia))
		{
			LOG.info("read NingXia config... ");
			path_app = "app_NingXia.xml";
			path_adapter = "xdr_data_adapter_NingXia.conf";
			path_event_adapter = "event_data_adapter_NingXia.conf";
		}
		else if (getCompile().Assert(CompileMark.BeiJing))
		{
			LOG.info("read BeiJing config... ");
			path_app = "app_BeiJing.xml";
			path_adapter = "xdr_data_adapter_BeiJing.conf";
			path_event_adapter = "event_data_adapter_BeiJing.conf";
		}
		else if (getCompile().Assert(CompileMark.ChongQing))
		{
			LOG.info("read ChongQing config... ");
			path_app = "app_ChongQing.xml";
			path_adapter = "xdr_data_adapter_ChongQing.conf";
			path_spark = "spark_ChongQing.xml";
		}
		else if (getCompile().Assert(CompileMark.HaiNan))
		{
			LOG.info("read HaiNan config... ");
			path_app = "app_HaiNan.xml";
			path_adapter = "xdr_data_adapter_HaiNan.conf";
			path_event_adapter = "event_data_adapter_HaiNan.conf";
		}
		else if (getCompile().Assert(CompileMark.QingHai))
		{
			LOG.info("read QingHai config... ");
			path_app = "app_QingHai.xml";
			path_adapter = "xdr_data_adapter_QingHai.conf";
			path_event_adapter = "event_data_adapter_QingHai.conf";
		}
		else if (getCompile().Assert(CompileMark.ShanXiJin))
		{
			LOG.info("read ShanXiJin config... ");
			path_app = "app_ShanXiJin.xml";
			path_adapter = "xdr_data_adapter_ShanXiJin.conf";
			path_event_adapter = "event_data_adapter_ShanXiJin.conf";
		}
		else if (getCompile().Assert(CompileMark.YunNan))
		{
			LOG.info("read YunNan config... ");
			path_app = "app_YunNan.xml";
			path_adapter = "xdr_data_adapter_YunNan.conf";
			path_event_adapter = "event_data_adapter_YunNan.conf";
		}
		else if (getCompile().Assert(CompileMark.TianJin))
		{
			LOG.info("read TianJin config... ");
			path_app = "app_TianJin.xml";
			path_adapter = "xdr_data_adapter_TianJin.conf";
			path_event_adapter = "event_data_adapter_TianJin.conf";
		}
		else if (getCompile().Assert(CompileMark.NeiMeng))
		{
			LOG.info("read NeiMeng config... ");
			path_app = "app_NeiMeng.xml";
			path_adapter = "xdr_data_adapter_NeiMeng.conf";
			path_event_adapter = "event_data_adapter_NeiMeng.conf";
		}
		else if (getCompile().Assert(CompileMark.XinJiang))
		{
			LOG.info("read XinJiang config... ");
			path_app = "app_XinJiang.xml";
			path_adapter = "xdr_data_adapter_XinJiang.conf";
			path_event_adapter = "event_data_adapter_XinJiang.conf";
		}
		else if (getCompile().Assert(CompileMark.ShanDong))
		{
			LOG.info("read ShanDong config... ");
			path_app = "app_ShanDong.xml";
			path_adapter = "xdr_data_adapter_ShanDong.conf";
			path_event_adapter = "event_data_adapter_ShanDong.conf";
		}

		IConfigure conf = null;
		try
		{
			
			{
				LOG.info("loading urlconfig.txt," + path_app + "," + path_adapter + "," + path_event_adapter + "...");

				InputStream inCfg = getClass().getClassLoader().getResourceAsStream(path_app);
				InputStream sparkCfg = null;
				try
				{
					sparkCfg = getClass().getClassLoader().getResourceAsStream(path_spark);
				}
				catch(Exception e)
				{
				}
				
				InputStream insDataAdapterConfig = getClass().getClassLoader().getResourceAsStream(path_adapter);
				InputStream insEventAdapterConfig = getClass().getClassLoader().getResourceAsStream(path_event_adapter);
				InputStream urlConfig = getClass().getClassLoader().getResourceAsStream("urlconfig.txt");

				if (inCfg == null)
				{
					LOG.info("inCfg is null");
					return false;
				}
				if (insDataAdapterConfig == null)
				{
					LOG.info("insDataAdapterConfig is null");
				}
				if (insEventAdapterConfig == null)
				{
					LOG.info("insEventAdapterConfig is null");
				}
				if (urlConfig == null)
				{
					LOG.info("urlConfig is null");
				}

				try
				{
					// app config
					conf = new StreamConfigure(inCfg);
					conf.loadConfigure();
					appConfig = new AppConfig(conf);
					
					// spark config
					if(sparkCfg != null)
					{
						conf = new StreamConfigure(sparkCfg);
						conf.loadConfigure();
						sparkConfig = new SparkConfig(conf);
					}

					// dataAdapter Config
					dataAdapterConfig = new DataAdapterConf();
					dataAdapterConfig.init(insDataAdapterConfig);

					// url cfg
					LocationParseService.setUrlConfig(urlConfig);

					eventAdapterConfig = new DataAdapterConf();
					try
					{
						eventAdapterConfig.init(insEventAdapterConfig);
					}
					catch (Exception e)
					{
						System.out.println("缺少eventAdater文件");
						e.printStackTrace();
					}

					
				}
				catch (Exception e)
				{
					e.printStackTrace();
					throw e;
				}
				finally
				{
					try
					{
						inCfg.close();
						insDataAdapterConfig.close();
						urlConfig.close();
						insEventAdapterConfig.close();
						sparkCfg.close();
					}
					catch (Exception e2)
					{
						// TODO: handle exception
					}

				}

				LOG.info("loading urlconfig.txt," + path_app + "," + path_adapter + "," + path_event_adapter
						+ " successed!");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			LOG.info("load urlconfig.txt," + path_app + "," + path_adapter + "," + path_event_adapter + " error :" + e.getMessage());
			return false;
		}

		return true;
	}

}