package cn.mastercom.bigdata.xdr.prepare.deal;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.io.Text;
import com.chinamobile.xdr.LocationInfo;
import com.chinamobile.xdr.demo;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.xdr.prepare.stat.XdrPrepareTablesEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.IDataDeal;

public class XdrPrepareDeal implements IDataDeal
{
	public static final int DataType_MAP = 10;
	public static final int DataType_REDUCE = 11;
	
	public static final int ImsiCount = 1;
	public static final int ImsiIP = 2;

	private String xmString = "";
	public StringBuffer sb = new StringBuffer();
	private String[] valstrs;
	private long imsi;
	private String userIP;
	private int userPort;
	private String serverIP;
	private String host;
	private String uri;
	private String HTTP_content_type = "";
	private String http_post_content = "";
	private String wifilist;
	
	private int eci;
//	private long msisdn;
	private Date dateTime;
//	private Map<Long, CPEUserItem> cpeMap = new HashMap<Long, CPEUserItem>();
	private String requestType = "POST";

	private ParseItem parseItem;
	private DataAdapterReader dataAdapterReader;
	private int splitMax = -1;
	private ResultOutputer resultOutputer;

	private DecimalFormat df = new DecimalFormat(".#######");
	private int total = 0;
	private Text key;

	public XdrPrepareDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
		init_once();
	}

	private void init_once()
	{
		try
		{
			parseItem = MainModel.GetInstance().getDataAdapterConfig().getParseItem("S1-HTTP");
			if (parseItem == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader = new DataAdapterReader(parseItem);

			splitMax = parseItem.getSplitMax("IMSI,User_IP_IPv4,User_Port,App_Server_IP_IPv4,HOST,Procedure_Start_Time,URI,HTTP_content_type,Cell_ID");
			if (splitMax < 0)
			{
				throw new IOException("time or imsi pos not right.");
			}
		}
		catch (IOException e)
		{
		}
	}

	public void init(Text key)
	{
		total = 0;
		this.key = key;
	}

	@Override
	public int pushData(int dataType, String value)
	{
		if (dataType == DataType_MAP) // map
		{
			try
			{
				xmString = new String(value.toString().getBytes(StaticConfig.UTFCode));
				valstrs = xmString.toString().split(parseItem.getSplitMark(), splitMax + 2);

				dataAdapterReader.readData(valstrs);
				imsi = dataAdapterReader.GetLongValue("IMSI", -1);
				//6061155539545534980L=""  5750288053043553775L=0
				if ((imsi + "").length() < 15 || imsi == 6061155539545534980L 
						|| imsi == 5750288053043553775L)
				{
					return StaticConfig.FAILURE;
				}
				userIP = dataAdapterReader.GetStrValue("User_IP_IPv4", "");
				serverIP = dataAdapterReader.GetStrValue("App_Server_IP_IPv4", "");
				host = dataAdapterReader.GetStrValue("HOST", "");
				dateTime = dataAdapterReader.GetDateValue("Procedure_Start_Time", new Date(1970, 1, 1));
				uri = dataAdapterReader.GetStrValue("URI", "");
				HTTP_content_type = dataAdapterReader.GetStrValue("HTTP_content_type", "");
				//add wifilist for 分层定位
				wifilist = dataAdapterReader.GetStrValue("wifi", "");
				try
				{
					eci = dataAdapterReader.GetIntValue("Cell_ID", -1);
					userPort = dataAdapterReader.GetIntValue("User_Port", -1);
				}
				catch (Exception e1)
				{
				}
//				msisdn = dataAdapterReader.GetLongValue("MSISDN", -1);
//				CPEUserItem cpeItem = cpeMap.get(imsi);
//				if (cpeItem == null)
//				{
//					cpeItem = new CPEUserItem();
//					cpeItem.imsi = imsi;
//					cpeItem.count = 0;
//					cpeItem.isCPE = 0;
//					cpeMap.put(imsi, cpeItem);
//				}
//
//				cpeItem.count++;
//				if (MainModel.GetInstance().getCompile().Assert(CompileMark.ShanXi))
//				{
//					if (HostMng.GetInstance().isXdrLocation(host))
//					{
//						try
//						{
//							// curTextKey.set("");
//							sb.delete(0, sb.length());
//							sb.append("");
//							sb.append("\t");
//							sb.append(imsi);
//							sb.append("\t");
//							sb.append(userIP);
//							sb.append("\t");
//							sb.append(userPort);
//							sb.append("\t");
//							sb.append(serverIP);
//							sb.append("\t");
//							sb.append(dateTime.getTime() / 1000L);
//							resultOutputer.pushData(ImsiIP, sb.toString());
//						}
//						catch (Exception e)
//						{
//							LOGHelper.GetLogger().writeLog(LogType.error, "format error：" + xmString, e);
//						}
//					}
//				}

				if (MainModel.GetInstance().getCompile().Assert(CompileMark.URI_ANALYSE))
				{
					List<LocationInfo> filledLocationInfoList = demo.DecryptLoc(requestType, host, uri, HTTP_content_type, http_post_content, false);
					
					for (LocationInfo locationInfo : filledLocationInfoList)
					{
						sb.delete(0, sb.length());
						sb.append("URI");
						sb.append("\t");
						sb.append(imsi);
						sb.append("|");
						sb.append(dateTime.getTime());
						sb.append("|");
						sb.append(dateTime.getTime());
						sb.append("|");
						sb.append(eci);
						sb.append("|");
						sb.append(userIP);
						sb.append("|");
						sb.append(userPort);
						sb.append("|");
						sb.append(serverIP);
						sb.append("|");
						sb.append(demo.GetLocation(host, locationInfo.locationType));
						sb.append("|");
						sb.append(demo.GetLoctp(locationInfo.locationType));
						sb.append("|");
						sb.append(locationInfo.radius);
						sb.append("|");
						sb.append(df.format(locationInfo.longitude));
						sb.append("|");
						sb.append(df.format(locationInfo.latitude));
						sb.append("|");
						sb.append(uri.length());
						sb.append("|");
						sb.append(wifilist);

						resultOutputer.pushData(XdrPrepareTablesEnum.xdrLocation.getIndex(), sb.toString());
					}
				}
			}
			catch (Exception e)
			{
				if (MainModel.GetInstance().getCompile().Assert(CompileMark.Debug))
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"format error：", "format error：" + xmString, e);
				}
			}
		}
		else if (dataType == DataType_REDUCE) // reduce
		{
			String[] valStr;
			valStr = value.toString().split("\t", -1);
			if (valStr.length == 3)
			{
				total += Integer.parseInt(valStr[1]);
			}
		}
		return StaticConfig.SUCCESS;
	}

	@Override
	public void statData()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void outData()
	{
		// TODO Auto-generated method stub
		if (total > 300000)
		{
			try
			{
				resultOutputer.pushData(ImsiCount, key.toString() + "\t" + total);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

}
