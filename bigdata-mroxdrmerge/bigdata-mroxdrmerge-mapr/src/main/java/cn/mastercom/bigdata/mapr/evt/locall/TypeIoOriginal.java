package cn.mastercom.bigdata.mapr.evt.locall;

import java.util.ArrayList;

import com.google.common.collect.ArrayListMultimap;

import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

/**
 * Created by Administrator on 2017/11/12.
 */
public class TypeIoOriginal
{
	public int xdrType;
	public String inputPath;
	public String aliasName;
	public String outputPath;

	public TypeIoOriginal(int xdrType, String inputPath, String aliasName, String outputPath)
	{
		this.xdrType = xdrType;
		this.inputPath = inputPath;
		this.aliasName = aliasName;
		this.outputPath = outputPath;
	}

	public void setInputPath(String inputPath)
	{
		this.inputPath = inputPath;
	}

	@Override
	public String toString()
	{
		return xdrType + "###" + inputPath + "###" + aliasName + "###" + outputPath + "\t";
	}

	public static ArrayList<TypeIoOriginal> fromString(String strs)
	{
		ArrayList<TypeIoOriginal> typeIoLists = new ArrayList<TypeIoOriginal>();
		try
		{
			String[] split = strs.split("\t");
			for (int i = 0; i < split.length; i++)
			{
				String[] typeioStrs = split[i].split("###");
				if (typeioStrs.length == 4)
				{
					int curXdrType = Integer.parseInt(typeioStrs[0]);
					String curInputPath = typeioStrs[1];
					String curAliasName = typeioStrs[2];
					String curOutputPath = typeioStrs[3];
					TypeIoOriginal ty = new TypeIoOriginal(curXdrType, curInputPath, curAliasName, curOutputPath);
					typeIoLists.add(ty);
				}

			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return typeIoLists;
	}

	public static ArrayListMultimap<Integer, TypeIoOriginal> regeist(String mroXdrMergePath, String statTime)
	{

		ArrayListMultimap<Integer, TypeIoOriginal> typeMaps = ArrayListMultimap.create();
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			String[] minuts = new String[] { "00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55" };
			for (int i = 0; i < hours.length; i++)
			{
				for (int j = 0; j < minuts.length; j++)
				{
					String mmePath = "/user/wangjun/S_O_DPI_LTE_S1_MME/load_time_d=20" + statTime.substring(3)
							+ "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
					String httpPath = "/user/wangjun/S_O_DPI_LTE_S1U_HTTP/load_time_d=20" + statTime.substring(3)
							+ "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
					String mgPath = "/user/wangjun/S_O_DPI_VL_MG/load_time_d=20" + statTime.substring(3)
							+ "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
					String svPath = "/user/wangjun/S_O_DPI_VL_SV/load_time_d=20" + statTime.substring(3)
							+ "/load_time_h=" + hours[i] + "/load_time_m=" + minuts[j] + "";
					{
						int type = TypeIoEvtEnum.ORIGINMME.getIndex();
						String aliasName = "tbEventOriginS1mme";
						String outpath = String.format("%1$s/xdr_locall/data_01_20%2$s/TB_EVENT_ORIGIN_S1MME_01_20%2$s",
								mroXdrMergePath, statTime.substring(3));
						typeMaps.put(type, new TypeIoOriginal(type, mmePath, aliasName, outpath));
					}

					{
						int type = TypeIoEvtEnum.ORIGINHTTP.getIndex();
						String aliasName = "tbEventOriginS1Http";
						String outpath = String.format("%1$s/xdr_locall/data_01_20%2$s/TB_EVENT_ORIGIN_S1HTTP_01_20%2$s",
								mroXdrMergePath, statTime.substring(3));
						typeMaps.put(type, new TypeIoOriginal(type, httpPath, aliasName, outpath));
					}

					{
						int type = TypeIoEvtEnum.ORIGINMG.getIndex();;
						String aliasName = String.format("tbEventOriginS1Mg");
						String outpath = String.format("%1$s/xdr_locall/data_01_20%2$s/TB_EVENT_ORIGIN_S1MG_01_20%2$s",
								mroXdrMergePath, statTime.substring(3));
						typeMaps.put(type, new TypeIoOriginal(type, mgPath, aliasName, outpath));
					}

					{
						int type = TypeIoEvtEnum.ORIGINSV.getIndex();;
						String aliasName = String.format("tbEventOriginS1NeiMengSv");
						String outpath = String.format("%1$s/xdr_locall/data_01_20%2$s/TB_EVENT_ORIGIN_S1NEIMENGSV_01_20%2$s",
								mroXdrMergePath, statTime.substring(3));
						typeMaps.put(type, new TypeIoOriginal(type, svPath, aliasName, outpath));
					}

				}
			}
			{
				int type = TypeIoEvtEnum.ORIGINRTP.getIndex();;
				String rtpPath = "/user/gaowei/rtp/20" + statTime.substring(3) + "";
				String aliasName = String.format("tbEventOriginS1rtp");
				String outpath = String.format("%1$s/xdr_locall/data_01_20%2$s/TB_EVENT_ORIGIN_S1RTP_01_20%2$s", mroXdrMergePath,
						statTime.substring(3));
				typeMaps.put(type, new TypeIoOriginal(type, rtpPath, aliasName, outpath));
			}

		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2))																		
		{

			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			String[] minuts = new String[] { "00", "15", "30", "45" };

			for (int i = 0; i < hours.length; i++)
			{
				for (int j = 0; j < minuts.length; j++)
				{
					String mmePath = "/OSSDATA/DPI_LTE/dr_s1_mme_tdr/20" + statTime.substring(3) + "/" + hours[i] + "/"
							+ minuts[j];
					String mwPath = "/OSSDATA/DPI_VOLTE/dr_volte_gm-mw_tdr/20" + statTime.substring(3) + "/" + hours[i]
							+ "/" + minuts[j];
					String svPath = "/OSSDATA/DPI_VOLTE/dr_volte_sv_tdr/20" + statTime.substring(3) + "/" + hours[i]
							+ "/" + minuts[j];
					String rxPath = "/OSSDATA/DPI_VOLTE/dr_volte_gx-rx_tdr/20" + statTime.substring(3) + "/" + hours[i]
							+ "/" + minuts[j];
					{
						int type = TypeIoEvtEnum.ORIGINMME.getIndex();;
						String aliasName = "tbEventOriginMme";
						String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MME_%2$s",
								mroXdrMergePath, statTime);
						typeMaps.put(type, new TypeIoOriginal(type, mmePath, aliasName, outpath));
					}

					{
						int type = TypeIoEvtEnum.ORIGINMW.getIndex();;
						String aliasName = "tbEventOriginMw";
						String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MW_%2$s",
								mroXdrMergePath, statTime);
						typeMaps.put(type, new TypeIoOriginal(type, mwPath, aliasName, outpath));
					}

					{
						int type = TypeIoEvtEnum.ORIGINSV.getIndex();;
						String aliasName = "tbEventOriginSv";
						String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_SV_%2$s",
								mroXdrMergePath, statTime);
						typeMaps.put(type, new TypeIoOriginal(type, svPath, aliasName, outpath));
					}

					{
						int type = TypeIoEvtEnum.ORIGINRX.getIndex();;
						String aliasName = "tbEventOriginRx";
						String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Rx_%2$s",
								mroXdrMergePath, statTime);
						typeMaps.put(type, new TypeIoOriginal(type, rxPath, aliasName, outpath));
					}

				}

			}

		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.XinJiang))
		{
			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			for (int k = 0; k < hours.length; k++)
			{
				int type = TypeIoEvtEnum.ORIGINHTTP.getIndex();;
				String httpPath = "/user/bdoc/24/services/hdfs/51/bs_cc_dpi_ltesdtp/HTTP/20"+statTime.substring(3)+"/"+hours[k];
//				String httpPath = "/seq/news1u_http_cut/20191120/"+hours[k];
				String aliasName = "tbEventOriginS1uHttp";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1U_HTTP_%2$s",
						mroXdrMergePath, statTime);
				typeMaps.put(type, new TypeIoOriginal(type, httpPath, aliasName, outpath));
			}
		}
		else
		{
			AppConfig appConfig = MainModel.GetInstance().getAppConfig();
			{
				int type = TypeIoEvtEnum.ORIGINHTTP.getIndex();
				String httpPath = String.format(appConfig.getHttpPath(), statTime.substring(3));

				String aliasName = "tbEventOriginS1uHttp";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_S1U_HTTP_%2$s",
						mroXdrMergePath, statTime);
				typeMaps.put(type, new TypeIoOriginal(type, httpPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINMME.getIndex();
				String mmePath = String.format(appConfig.getMmePath(), statTime.substring(3));
				String aliasName = "tbEventOriginMme";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MME_%2$s", mroXdrMergePath,
						statTime);

				typeMaps.put(type, new TypeIoOriginal(type, mmePath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINMW.getIndex();
				String aliasName = "tbEventOriginMw";
				String mwPath = String.format(appConfig.getMwPath(), statTime.substring(3));
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MW_%2$s", mroXdrMergePath,
						statTime);
				typeMaps.put(type, new TypeIoOriginal(type, mwPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINSV.getIndex();
				String svPath = String.format(appConfig.getSvPath(), statTime.substring(3));
				String aliasName = "tbEventOriginSv";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_SV_%2$s", mroXdrMergePath,
						statTime);
				typeMaps.put(type, new TypeIoOriginal(type, svPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINRX.getIndex();
				String rxPath = String.format(appConfig.getRxPath(), statTime.substring(3));

				String aliasName = "tbEventOriginRx";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Rx_%2$s", mroXdrMergePath,
						statTime);

				typeMaps.put(type, new TypeIoOriginal(type, rxPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGIN_Uu.getIndex();
				String uuPath = String.format(appConfig.getUuPath(), statTime.substring(3));
				String aliasName = "tbEventOriginUu";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_UU_%2$s", mroXdrMergePath,
						statTime);

				typeMaps.put(type, new TypeIoOriginal(type, uuPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINMOS_BEIJING.getIndex();
				String mosPath = String.format(appConfig.getMosPath(), statTime.substring(3));
				String aliasName = "tbEventOriginMosBeiJing";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MOS_BEIJING_%2$s",
						mroXdrMergePath, statTime);

				typeMaps.put(type, new TypeIoOriginal(type, mosPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINWJTDH_BEIJING.getIndex();
				String wjtDhPath = String.format(appConfig.getDhwjtPath(), statTime.substring(3));
				String aliasName = "tbEventOriginMjtdhBeiJing";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_MJTDH_BEIJING_%2$s",
						mroXdrMergePath, statTime);

				typeMaps.put(type, new TypeIoOriginal(type, wjtDhPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINIMS_MO.getIndex();
				String imsMoPath = String.format(appConfig.getImsMoPath(), statTime.substring(3));
				String aliasName = "tbEventOriginImsMo";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Ims_Mo_%2$s",
						mroXdrMergePath, statTime);

				typeMaps.put(type, new TypeIoOriginal(type, imsMoPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINIMS_MT.getIndex();
				String imsMtPath = String.format(appConfig.getImsMtPath(), statTime.substring(3));
				String aliasName = "tbEventOriginImsMt";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Ims_Mt_%2$s",
						mroXdrMergePath, statTime);

				typeMaps.put(type, new TypeIoOriginal(type, imsMtPath, aliasName, outpath));
			}

			{
				int type = TypeIoEvtEnum.ORIGINCDR_QUALITY.getIndex();
				String qualityPaath = String.format(appConfig.getQuaLityPath(), statTime.substring(3));
				String aliasName = "tbEventOriginCdrQuality";
				String outpath = String.format("%1$s/xdr_locall/data_%2$s/TB_EVENT_ORIGIN_Cdr_Quality_%2$s",
						mroXdrMergePath, statTime);

				typeMaps.put(type, new TypeIoOriginal(type, qualityPaath, aliasName, outpath));
			}
		}

		return typeMaps;
	}

}
