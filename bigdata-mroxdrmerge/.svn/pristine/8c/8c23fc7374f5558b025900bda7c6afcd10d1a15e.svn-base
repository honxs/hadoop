package cn.mastercom.bigdata.locuser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.StaticConfig;

public class UserProp
{

	public Map<String/* eci+apid */, ArrayList<MrSam>> ecis = new HashMap<String, ArrayList<MrSam>>();

	public List<MrSplice> samprop = new ArrayList<MrSplice>();

	public CfgInfo cInfo = null;

	public Map<String/* eci+fcn+pci */, Integer> figs = new HashMap<String, Integer>();

	private UserLoca ul = new UserLoca();

	public Configuration conf = null;

	public UserProp()
	{

	}

	public UserProp(Configuration conf)
	{
		this.conf = conf;
	}

	public void DoWork(long eci, List<SIGNAL_MR_All> lSams)
	{
		if (cInfo == null)
		{
			cInfo = new CfgInfo();
			if (!cInfo.Init(conf))
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "cInfo==null");
				cInfo.Clear();
				cInfo = null;
				return;
			}
		}

		samprop.clear();
		ecis.clear();
		figs.clear();

		SetSams(lSams);

		if (ecis.size() > 0)
		{
			SetCell();

			SetProp();

			SetPosi();
		}

		samprop.clear();
		ecis.clear();
		figs.clear();
	}

	private void SetSams(List<SIGNAL_MR_All> lsams)
	{
		for (int ii = 0; ii < lsams.size(); ii++)
		{
			SIGNAL_MR_All mall = lsams.get(ii);

			String skey = String.valueOf(mall.tsc.Eci) + "_" + String.valueOf(mall.tsc.MmeUeS1apId);

			if (!ecis.containsKey(skey))
			{
				ecis.put(skey, new ArrayList<MrSam>());
			}

			// 过滤指纹库用
			String sfig = String.valueOf(mall.tsc.Eci) + "_0_0";
			if (!figs.containsKey(sfig))
			{
				figs.put(sfig, 0);
			}

			List<MrSam> msam = ecis.get(skey);

			MrSam sam = new MrSam();
			sam.mall = mall;
			sam.itime = mall.tsc.beginTime;
			sam.cityid = mall.tsc.cityID;
			sam.eci = (int) mall.tsc.Eci;
			sam.MmeUeS1apId = mall.tsc.MmeUeS1apId;
			sam.mrotype = mall.tsc.EventType;

			Mrcell scell = new Mrcell();
			scell.cell = sam.eci * (-1);
			scell.btime = sam.itime;
			scell.etime = sam.itime;
			scell.rsrp_avg = mall.tsc.LteScRSRP;
			scell.rsrq_avg = mall.tsc.LteScRSRQ;
			scell.isscell = 1;
			if (scell.rsrp_avg != -1000000)
			{
				scell.rsrp_cnt = 1;
			}
			if (scell.rsrq_avg != -1000000)
			{
				scell.rsrq_cnt = 1;
			}
			sam.cells.put(scell.cell, scell);

			for (int kk = 0; kk < mall.nccount[0]; kk++)
			{
				NC_LTE SamField = mall.tlte[kk];

				Mrcell ncell = new Mrcell();
				ncell.rsrp_avg = SamField.LteNcRSRP;
				ncell.rsrq_avg = SamField.LteNcRSRQ;
				int earfcn = SamField.LteNcEarfcn;
				int pci = SamField.LteNcPci;

				if (earfcn != -1000000 && pci != -1000000 && pci != 0 && earfcn != 0)
				{
					String ssfig = "0_" + String.valueOf(earfcn) + "_" + String.valueOf(pci);
					if (!figs.containsKey(ssfig))
					{
						figs.put(ssfig, 0);
					}

					if (ncell.rsrp_avg != -1000000)
					{
						ncell.btime = sam.itime;
						ncell.etime = sam.itime;
						ncell.cell = (pci << 16) + earfcn;
						ncell.rsrp_cnt = 1;
						if (ncell.rsrq_avg != -1000000)
						{
							ncell.rsrq_cnt = 1;
						}
						sam.cells.put(ncell.cell, ncell);
					}
				}
			}
			msam.add(sam);
		}
	}

	private void SetCell()
	{
		LOGHelper.GetLogger().writeLog(LogType.info, "cell prop..." + ecis.size());

		CfgCell sc = null;

		for (ArrayList<MrSam> mm : ecis.values())
		{
			sc = null;
			for (int ii = 0; ii < mm.size(); ii++)
			{
				if (sc == null)
				{
					sc = cInfo.GetCell(mm.get(ii).eci);

					if (sc == null)
					{
						break;
					}
				}

				for (Mrcell mc : mm.get(ii).cells.values())
				{
					if (mc.isscell == 1)
					{
						if (sc != null)
						{
							mc.neci = sc.eci;
							mc.btsid = sc.btsid;
							mc.isindoor = sc.isindoor;
							mc.longitude = sc.longitude;
							mc.latitude = sc.latitude;
							mc.iheight = sc.height; // add by yht
							mc.btsid = sc.eci / 256; // add by yht 调整为enbid
						}
					}
					else
					{
						CfgCell cc = cInfo.GetNCell(mm.get(ii).eci, mc.cell);
						if (cc != null)
						{
							mc.neci = cc.eci;
							mc.btsid = cc.btsid;
							mc.isindoor = cc.isindoor;
							mc.longitude = cc.longitude;
							mc.latitude = cc.latitude;
							mc.iheight = cc.height; // add by yht
						}
						else
						{
							CfgCell cl = cInfo.GetNCell(mc.cell, sc.eci, sc.longitude, sc.latitude, mm.get(ii).cityid);
							if (cl != null)
							{
								mc.dist = 0;
								mc.neci = cl.eci;
								mc.btsid = cl.btsid;
								mc.isindoor = cl.isindoor;
								mc.longitude = cl.longitude;
								mc.latitude = cl.latitude;
								mc.iheight = cl.height; // add by yht
							}
						}
					}
				}
			}
		}
	}

	private void SetProp()
	{
		LOGHelper.GetLogger().writeLog(LogType.info, "section analyse...");

		for (ArrayList<MrSam> mm : ecis.values())
		{
			Collections.sort(mm, new Comparator<MrSam>()
			{
				@Override
				public int compare(final MrSam b1, final MrSam b2)
				{
					return (b1.itime - b2.itime);
				}
			});
			// 取一个段落
			int ii = 0;
			int jj = 0;
			for (jj = 0; jj < mm.size() - 1; jj++)
			{
				if (mm.get(jj + 1).itime - mm.get(jj).itime <= 15)
				{
					continue;
				}

				DealSection(mm, ii, jj);

				ii = jj + 1;
			}

			DealSection(mm, ii, jj);
		}
	}

	private void DealSection(ArrayList<MrSam> mm, int spos, int epos)
	{
		// 记录该section的起始位置
		int lcisn_start = samprop.size();
		int lcischanged = 0;// 记录是否一个session中的分类结果变化了
		int lcifirstprop = 0;
		int lcissetprop = 0; // 是否已经设置了室内外

		// 构造矩阵表
		HashMap<Integer/* 小区 */, TimePoint/* 全部聚合时间点 */> cells = new HashMap<Integer, TimePoint>();
		for (int ii = spos; ii <= epos; ii++)
		{
			MakeMatrix(mm.get(ii), cells, ii - spos, epos - spos + 1);
		}
		// 填充所有小区的全部聚合时间点
		for (int ii = spos; ii <= epos; ii++)
		{
			for (TimePoint mout : cells.values())
			{
				mout.celltimes.get(ii - spos).itime = mm.get(ii).itime;
			}
		}

		if (cells.size() > 0)
		{
			int ismoving = 0;
			int nCount = cells.entrySet().iterator().next().getValue().celltimes.size();
			// 切片判断,每个时间段判断所有小区，有变化切为一个片
			int jj = 0;
			int kk = 0;
			for (kk = 0; kk < nCount; kk++)
			{
				// 针对一个时间点，处理所有小区
				for (TimePoint ncell : cells.values())
				{
					if (GetStatus(kk, ncell))
					{
						ismoving = 1;
						OutSplice(jj, kk - 1, cells, mm.get(spos).itime, mm.get(epos).itime, ismoving);

						MrSplice lms = samprop.get(samprop.size() - 1);
						if ((lms.doortype == 1) || (lms.doortype == 2))
						{
							lcissetprop = 1;
						}
						if (lcifirstprop == 0)
						{
							lcifirstprop = lms.property;
						}
						else
						{
							if (lcifirstprop != lms.property)
							{
								lcischanged = lcischanged + 1;
							}
						}
						jj = kk;
						// 要从当前点重新开始计算
						kk = kk - 1;
						break;
					}
				}
			}

			// 最后一段
			OutSplice(jj, kk - 1, cells, mm.get(spos).itime, mm.get(epos).itime, ismoving);

			MrSplice lsms = samprop.get(samprop.size() - 1);
			if ((lsms.doortype == 1) || (lsms.doortype == 2))
			{
				lcissetprop = 1;
			}
			if (lcifirstprop > 0 && (lcifirstprop != lsms.property))
			{
				lcischanged = lcischanged + 1;
			}
		}

		// 处理单独一个session的记录
		int lcisn_end = samprop.size();
		if (lcischanged > 0)
		{
			DealSessionProperty(lcisn_start, lcisn_end);
		}

		if (lcissetprop == 0)
		{
			// 需要处理未识别的部分
			DealSessionProperty_unknown(lcisn_start, lcisn_end);
		}
	}

	private void DealSessionProperty_unknown(int sn_start, int sn_end)
	{
		// 处理定位完成后没有识别的部分
		int lciproperty_end = 0;
		int lcisindoor_end = -1;
		int lcismove_end = -1;
		int lcishaveindoorcell_end = -1;

		int iproperty = samprop.get(sn_start).property;
		lcismove_end = (iproperty - iproperty / 10 * 10);
		lcisindoor_end = (iproperty / 10 - iproperty / 100 * 10);
		lcishaveindoorcell_end = iproperty / 100;

		for (int ii = sn_start; ii < sn_end; ii++)
		{
			if (samprop.get(ii).doortype == 3)
			{
				lcisindoor_end = 3;
			}
			else
			{
				if ((lcisindoor_end <= 0) && (samprop.get(ii).doortype == 4))
				{
					lcisindoor_end = 4;
				}
			}
		}
		lciproperty_end = lcishaveindoorcell_end * 100 + lcisindoor_end * 10 + lcismove_end;
		for (int ii = sn_start; ii < sn_end; ii++)
		{
			samprop.get(ii).property = lciproperty_end;
			samprop.get(ii).doortype = lcisindoor_end; // 因为后面还有3和4是未完成时替换的
		}
	}

	private void DealSessionProperty(int sn_start, int sn_end)
	{
		if ((sn_end - sn_start) <= 1)
		{
			// 只有1个，不用设置了
			return;
		}
		// 先识别到底是什么类型的
		int lciproperty_end = 0;
		int lcisindoor_end = -1;
		int lcismove_end = -1;
		int lcishaveindoorcell_end = -1;
		int lcisindoor = -1;
		int lcismove = -1;
		int lcishaveindoorcell = -1;

		int lcishave_outdoor = -1; // 记录是否定位到室外了

		for (int ii = sn_start; ii < sn_end; ii++)
		{
			int iproperty = samprop.get(ii).property;
			lcismove = (iproperty - iproperty / 10 * 10);
			lcisindoor = (iproperty / 10 - iproperty / 100 * 10);
			lcishaveindoorcell = iproperty / 100;

			if (lcisindoor == 1) // 只要有一个室内，即定为室内
			{
				lcisindoor_end = 1;
			}
			else if (lcisindoor == 2)
			{
				lcishave_outdoor = 1;// 记录有定位到室外的
			}
			if (lcismove == 1)// 只要有一个是移动，即定为移动
			{
				lcismove_end = 1;
			}
			if (lcishaveindoorcell == 1)// 只要有一个有室分，即定为含室分
			{
				lcishaveindoorcell_end = 1;
			}
		}
		// 统一赋值
		if (lcishaveindoorcell_end == -1)
		{
			lcishaveindoorcell_end = 2; // 表明没有室分
		}
		if (lcismove_end == -1)
		{
			lcismove_end = 2; // 表明没有移动
		}
		if (lcisindoor_end == -1) // 表明没有定到室内
		{
			if (lcishave_outdoor == 1)
			{
				lcisindoor_end = 2; // 设置为室外
			}
			else
			{
				lcisindoor_end = 0;
			}
		}
		lciproperty_end = lcishaveindoorcell_end * 100 + lcisindoor_end * 10 + lcismove_end;
		for (int ii = sn_start; ii < sn_end; ii++)
		{
			samprop.get(ii).property = lciproperty_end;
			if (lcisindoor_end > 0)
			{
				samprop.get(ii).doortype = lcisindoor_end; // 因为后面还有3和4是未完成时替换的
			}
		}
	}

	private boolean GetStatus(int bb, TimePoint tp)
	{
		if (tp.celltimes.get(bb).cell.cell == -1)
		{
			return false;
		}

		if (tp.rsrp_max == -1000000)
		{
			tp.rsrp_max = tp.celltimes.get(bb).rsrp_max;
			tp.rsrp_min = tp.rsrp_max;
		}
		else
		{
			if (tp.celltimes.get(bb).rsrp_max != -1000000)
				tp.rsrp_max = Math.max(tp.celltimes.get(bb).rsrp_max, tp.rsrp_max);
			if (tp.celltimes.get(bb).rsrp_min != -1000000)
				tp.rsrp_min = Math.min(tp.celltimes.get(bb).rsrp_min, tp.rsrp_min);

			if (Math.abs(tp.rsrp_max - tp.rsrp_min) > 8)
			{
				return true;
			}
		}

		return false;
	}

	private void OutSplice(int sp, int ep, HashMap<Integer, TimePoint> nls, int section_btime, int section_etime, int moving)
	{
		MrSplice splice = new MrSplice();
		splice.section_btime = section_btime;
		splice.section_etime = section_etime;
		List<MrPoint> mp = nls.entrySet().iterator().next().getValue().celltimes;
		splice.splice_btime = mp.get(sp).itime;
		splice.splice_etime = mp.get(ep).itime;
		splice.eci = mp.get(sp).eci;
		splice.MmeUeS1apId = mp.get(sp).MmeUeS1apId;
		splice.itime = splice.splice_btime;
		splice.ismoving = moving;

		for (TimePoint lm : nls.values())
		{
			// 清理本次统计
			lm.rsrp_max = -1000000;
			lm.rsrp_min = -1000000;

			MrPoint ncell = null;
			for (int ii = sp; ii <= ep; ii++)
			{
				if (lm.celltimes.get(ii).cell.cell == -1)
				{
					continue;
				}

				if (ncell == null)
				{
					ncell = new MrPoint();

					ncell.itime = lm.celltimes.get(ii).itime;
					ncell.eci = lm.celltimes.get(ii).eci;
					ncell.MmeUeS1apId = lm.celltimes.get(ii).MmeUeS1apId;
					ncell.cell.btime = lm.celltimes.get(ii).cell.btime;
					ncell.cell.etime = lm.celltimes.get(ii).cell.etime;
					ncell.cell.isscell = lm.celltimes.get(ii).cell.isscell;
					ncell.cell.isindoor = lm.celltimes.get(ii).cell.isindoor;
					ncell.cell.btsid = lm.celltimes.get(ii).cell.btsid;
					ncell.cell.neci = lm.celltimes.get(ii).cell.neci;
					ncell.cell.dist = lm.celltimes.get(ii).cell.dist;
					ncell.cell.longitude = lm.celltimes.get(ii).cell.longitude;
					ncell.cell.latitude = lm.celltimes.get(ii).cell.latitude;
					ncell.cell.cell = lm.celltimes.get(ii).cell.cell;
					ncell.cell.rsrp_cnt = lm.celltimes.get(ii).cell.rsrp_cnt;
					ncell.cell.rsrp_avg = lm.celltimes.get(ii).cell.rsrp_avg;
					ncell.cell.rsrq_cnt = lm.celltimes.get(ii).cell.rsrq_cnt;
					ncell.cell.rsrq_avg = lm.celltimes.get(ii).cell.rsrq_avg;
					ncell.rsrp_max = lm.celltimes.get(ii).rsrp_max;
					ncell.rsrp_min = lm.celltimes.get(ii).rsrp_min;
					ncell.rsrq_max = lm.celltimes.get(ii).rsrq_max;
					ncell.rsrq_min = lm.celltimes.get(ii).rsrq_min;
					ncell.cell.iheight = lm.celltimes.get(ii).cell.iheight;// add
																			// by
																			// yht
				}
				else
				{
					ncell.cell.etime = Math.max(ncell.cell.etime, lm.celltimes.get(ii).cell.etime);
					ncell.cell.rsrp_cnt += lm.celltimes.get(ii).cell.rsrp_cnt;
					if (ncell.cell.rsrp_avg == -1000000)
						ncell.cell.rsrp_avg = lm.celltimes.get(ii).cell.rsrp_avg;
					else
					{
						if (lm.celltimes.get(ii).cell.rsrp_avg != -1000000)
							ncell.cell.rsrp_avg += lm.celltimes.get(ii).cell.rsrp_avg;
					}

					if (ncell.rsrp_max == -1000000)
						ncell.rsrp_max = lm.celltimes.get(ii).rsrp_max;
					else
					{
						if (lm.celltimes.get(ii).rsrp_max != -1000000)
							ncell.rsrp_max = Math.max(lm.celltimes.get(ii).rsrp_max, ncell.rsrp_max);
					}

					if (ncell.rsrp_min == -1000000)
						ncell.rsrp_min = lm.celltimes.get(ii).rsrp_min;
					else
					{
						if (lm.celltimes.get(ii).rsrp_min != -1000000)
							ncell.rsrp_min = Math.min(lm.celltimes.get(ii).rsrp_min, ncell.rsrp_min);
					}

					ncell.cell.rsrq_cnt += lm.celltimes.get(ii).cell.rsrq_cnt;
					if (ncell.cell.rsrq_avg == -1000000)
						ncell.cell.rsrq_avg = lm.celltimes.get(ii).cell.rsrq_avg;
					else
					{
						if (lm.celltimes.get(ii).cell.rsrq_avg != -1000000)
							ncell.cell.rsrq_avg += lm.celltimes.get(ii).cell.rsrq_avg;
					}

					if (ncell.rsrq_max == -1000000)
						ncell.rsrq_max = lm.celltimes.get(ii).rsrq_max;
					else
					{
						if (lm.celltimes.get(ii).rsrq_max != -1000000)
							ncell.rsrq_max = Math.max(lm.celltimes.get(ii).rsrq_max, ncell.rsrq_max);
					}

					if (ncell.rsrq_min == -1000000)
						ncell.rsrq_min = lm.celltimes.get(ii).rsrq_min;
					else
					{
						if (lm.celltimes.get(ii).rsrq_min != -1000000)
							ncell.rsrq_min = Math.min(lm.celltimes.get(ii).rsrq_min, ncell.rsrq_min);
					}
				}
			}

			if (ncell != null)
			{
				splice.cells.add(ncell);
			}
		}

		// 采样点小区先排序
		// modified by yht 修改按照btsid进行排序,然后进行是否同站的属性的设置
		// splice.cells.Sort((a, b) => ((b.cell.rsrp_avg / b.cell.rsrp_cnt ==
		// a.cell.rsrp_avg / a.cell.rsrp_cnt) ?
		// b.cell.neci.CompareTo(a.cell.neci) : (b.cell.rsrp_avg /
		// b.cell.rsrp_cnt).CompareTo(a.cell.rsrp_avg / a.cell.rsrp_cnt)));

		Collections.sort(splice.cells, new Comparator<MrPoint>()
		{
			public int compare(MrPoint arg1, MrPoint arg2)
			{
				return (arg1.cell.btsid - arg2.cell.btsid);
			}
		});

		for (int ii = 0; ii < splice.cells.size() - 1; ii++)
		{
			if (splice.cells.get(ii).cell.btsid == splice.cells.get(ii + 1).cell.btsid)
			{
				splice.cells.get(ii).cell.issamebts = 1;
				splice.cells.get(ii + 1).cell.issamebts = 1;
			}
			// add by yht begin 设置距离限制
			splice.cells.get(ii).FSetDisLimit();
			// add by yht end
		}
		splice.cells.get(splice.cells.size() - 1).FSetDisLimit();
		// modified by yht end

		splice.property = GetProperty(splice);

		samprop.add(splice);
	}

	private int GetProperty(MrSplice splice)
	{
		double s_rsrp = -1000000; // 服务小区
		double n_max_rsrp = -1000000; // 最强邻区
		double n_max_out_rsrp = -1000000; // 最强非室内邻区
		double n_max_in_rsrp = -1000000; // 最强室内邻区
		int lcicnt_f90 = 0;// 场强超过-90的室外邻区个数
		int a_long = 0;
		int cnt = 0;

		for (MrPoint lm : splice.cells)
		{
			double rsrp = lm.cell.rsrp_avg / lm.cell.rsrp_cnt;
			a_long = Math.max(a_long, lm.cell.etime - lm.cell.btime);

			if (lm.cell.isscell == 1)
			{
				splice.scell = lm;
				s_rsrp = rsrp;
			}
			else
			{
				n_max_rsrp = Math.max(rsrp, n_max_rsrp);
				cnt += lm.cell.rsrp_cnt;

				if (lm.cell.isindoor == 1)
				{
					if (rsrp > n_max_in_rsrp)
					{
						splice.incell = lm;
						n_max_in_rsrp = rsrp;
					}
					if (lm.rsrp_max >= -90)
					{
						lcicnt_f90++;
					}
				}
				else
				{
					if (rsrp > n_max_out_rsrp)
					{
						splice.outeci = lm.cell.neci;
						n_max_out_rsrp = rsrp;
					}
				}
			}
		}

		splice.doortype = 0;
		if (splice.scell != null)
		{
			if (splice.scell.cell.isindoor == 0)
			{
				// 用服务小区
				splice.outeci = splice.scell.cell.neci;
			}
			else
			{
				int eci = cInfo.GetOut(splice.scell.cell.neci);
				if (eci > 0)
				{
					splice.outeci = eci;
				}
			}

			// 室内：
			// 1 case when isindoor=1 and rsrp_avg-nrsrp_avg_nindoor >= 0 then 1
			// else 0 end AS isindoor1 服务小区为室内，且服务小区场强比最强非室内邻区强
			// 2 case when nrsrp_avg_indoor-nrsrp_avg_nindoor >=0 and
			// nrsrp_avg_indoor-rsrp_avg >=0 and nrsrp_avg_indoor >=-100 then 1
			// else 0 end AS isindoor2
			// 邻区为室内，且邻区场强比非室内最强邻区强，并且比服务小区信号强，同时满足 信号>=-100dB
			// 3 case when isindoor+isnindoor=1 and ncellnum <= 2 and
			// iscellmoving+isncellmoving=0 then 1 else 0 end AS isindoor5
			// 服务小区或邻区为室内，且邻区数量少于2个，且没有发生移动现象; update :移动现象去掉
			// 4 case when (isindoor+isnindoor=0) and
			// (rsrp_avg>nrsrp_avg_nindoor+10) and rsrp_avg<-90 and
			// nrsrp_avg_nindoor<-100 and iscellmoving+isncellmoving=0 then 1
			// else 0 end AS isindoor3
			// 主服和邻区都没有室内站信号，服务小区比邻区强10dB以上，并且满足服务小区<-90
			// ,邻区小于-100，并且服务小区和邻区都没有移动 update :移动现象去掉
			// 5 case when (isindoor+isnindoor=0) and splice_etime-splice_btime
			// > =15 and iscellmoving+isncellmoving=0 and (ncellnum <= 3 OR
			// nsamplenumper <=1) then 1 else 0 end AS isindoor4
			// 主服和邻区都没有室内站信号，切片跨度长大于等于15秒，没有发生移动，且邻小区数小于等于2 或者是平均每采样点邻区数小于等于1
			// update :移动现象去掉
			if (((splice.scell.cell.isindoor == 1) && (s_rsrp > n_max_out_rsrp)) || (splice.incell != null && n_max_in_rsrp > n_max_out_rsrp && n_max_in_rsrp > s_rsrp && n_max_in_rsrp >= -100)
					|| ((splice.incell != null || splice.scell.cell.isindoor == 1) && splice.cells.size() <= 3)
					|| (((splice.incell == null) || splice.scell.cell.isindoor == 0) && s_rsrp - n_max_rsrp >= 10 && s_rsrp <= -90 && n_max_rsrp <= -100)
					|| (((splice.incell == null) || splice.scell.cell.isindoor == 0) && a_long >= 15 && (splice.cells.size() <= 3 || cnt / splice.scell.cell.rsrp_cnt <= 1)))
			{
				splice.doortype = 1;
			}
			else
			{
				// 室外：
				// 1 case when (isindoor+isnindoor=0) and rsrp_max>-80 and
				// ncellnum >= 2 then 1 else 0 end as isoutmov1
				// 无室分邻区和服务小区，且服务小区信号场强大于-80，且邻区个数大于等于2
				// 2 case when isindoor+isnindoor=0 and
				// ((iscellmoving+isncellmoving>=1) and (rsrp_max>=-85 OR
				// nrsrp_avg_nindoor>=-85)) then 1 else 0 end AS isoutmov2
				// 无室分邻区和服务小区，且服务小区或邻区中有移动，服务小区中的max大于等于-85 或者是 非室分邻区大于等于-85
				// 3 case when isnindoor=1 and nrsrp_avg_indoor <=-95 and
				// (rsrp_avg>nrsrp_avg_indoor or nrsrp_avg_nindoor >
				// nrsrp_avg_indoor) then 1 else 0 end as isoutmov3
				// 邻区含室分信号，且邻区室分最强信号小于等于-95dB ，且室外小区信号大于室内小区信号
				// 4 case when (isindoor+isnindoor=0) and
				// iscellmoving+isncellmoving>=1 and rsrp_avg > -90 and
				// nrsrp_avg_nindoor >-90 then 1 else 0 end as isoutmov4
				// 无室分邻区和服务小区，服务小区或邻区有移动，且服务小区和最强邻区都大于等于-90dB
				// update:移动去掉，增加，且邻区数>=3
				// 5 case when isindoor=1 and rsrp_avg <=-95 and
				// (nrsrp_avg_nindoor > rsrp_avg) then 1 else 0 end as isoutmov5
				// 主服为室分，主服场强小于等于-95，并且最强邻区大于主服小区 室外场强超过-90dB的邻区数超过2个
				if ((((splice.incell == null) || splice.scell.cell.isindoor == 0) && s_rsrp >= -80 && splice.cells.size() >= 3)
						|| (((splice.incell == null) || splice.scell.cell.isindoor == 0) && splice.ismoving == 1 && s_rsrp >= -85 && n_max_out_rsrp >= -85)
						|| (splice.incell != null && n_max_in_rsrp <= -95 && (n_max_out_rsrp > n_max_in_rsrp || (splice.scell.cell.isindoor == 1 ? s_rsrp : n_max_out_rsrp) > n_max_in_rsrp))
						|| (((splice.incell == null) || splice.scell.cell.isindoor == 0) && s_rsrp >= -90 && n_max_rsrp >= -90 && splice.cells.size() >= 3)
						|| (splice.scell.cell.isindoor == 1 && s_rsrp <= -95 && n_max_rsrp > s_rsrp) || (lcicnt_f90 >= 2))
				{
					splice.doortype = 2;
				}
			}
		}

		if (splice.doortype == 1)
		{
			if (splice.ismoving == 1)
			{
				return (splice.incell != null || splice.scell.cell.isindoor == 1) ? Property.move_indoor_ccell : Property.move_indoor_ccell_no;
			}
			else
			{
				return (splice.incell != null || splice.scell.cell.isindoor == 1) ? Property.static_indoor_ccell : Property.static_indoor_ccell_no;
			}
		}
		else if (splice.doortype == 2)
		{
			if (splice.ismoving == 1)
			{
				return (splice.incell != null || splice.scell.cell.isindoor == 1) ? Property.move_outdoor_ccell : Property.move_outdoor_ccell_no;
			}
			else
			{
				return (splice.incell != null || splice.scell.cell.isindoor == 1) ? Property.static_outdoor_ccell : Property.static_outdoor_ccell_no;
			}
		}
		else
		{
			// 在未定位上的情况下，
			if (((splice.cells.size() <= 3) && (n_max_out_rsrp <= -90)) || ((splice.cells.size() >= 3) && (n_max_out_rsrp < -95) && (splice.scell.rsrp_max < -95)))
			{
				splice.doortype = 3; // 未知转室内
			}
			else
			{
				splice.doortype = 4; // 剩余未知的全部归为室外
			}

			if (splice.ismoving == 1)
			{
				return (splice.incell != null || splice.scell.cell.isindoor == 1) ? Property.move_unknown_ccell : Property.move_unknown_ccell_no;
			}
			else
			{
				return (splice.incell != null || splice.scell.cell.isindoor == 1) ? Property.static_unknown_ccell : Property.static_unknown_ccell_no;
			}
		}
	}

	private void MakeMatrix(MrSam rsam, HashMap<Integer, TimePoint> mmcells, int tpos, int cn)
	{
		// time1 time2 ....
		// cell1
		// cell2
		// ....
		for (Mrcell rcell : rsam.cells.values())
		{
			if (!mmcells.containsKey(rcell.cell))
			{
				// 所有时间位都设置上
				TimePoint tp = new TimePoint();

				for (int kk = 0; kk < cn; kk++)
				{
					MrPoint lcc = new MrPoint();
					lcc.eci = rsam.eci;
					lcc.itime = rsam.itime; // 聚合时间点
					lcc.MmeUeS1apId = rsam.MmeUeS1apId;
					lcc.cell.cell = -1;
					tp.celltimes.add(lcc);
				}
				mmcells.put(rcell.cell, tp);
			}

			TimePoint ll = mmcells.get(rcell.cell);

			MrPoint lc = ll.celltimes.get(tpos);
			lc.itime = rsam.itime;
			lc.cell.btime = rcell.btime;
			lc.cell.etime = rcell.etime;
			lc.cell.status = rcell.status;
			lc.cell.cell = rcell.cell;
			lc.cell.isindoor = rcell.isindoor;
			lc.cell.btsid = rcell.btsid;
			lc.cell.neci = rcell.neci;
			lc.cell.dist = rcell.dist;
			lc.cell.longitude = rcell.longitude;
			lc.cell.latitude = rcell.latitude;
			lc.cell.isscell = rcell.isscell;
			lc.cell.rsrp_cnt = rcell.rsrp_cnt;
			lc.cell.rsrp_avg = rcell.rsrp_avg;
			lc.cell.rsrq_cnt = rcell.rsrq_cnt;
			lc.cell.rsrq_avg = rcell.rsrq_avg;
			lc.cell.iheight = rcell.iheight; // add by yht

			if (rcell.rsrp_avg != -1000000)
			{
				lc.rsrp_max = rcell.rsrp_avg / rcell.rsrp_cnt;
				lc.rsrp_min = lc.rsrp_max;
			}
			if (rcell.rsrq_avg != -1000000)
			{
				lc.rsrq_max = rcell.rsrq_avg / rcell.rsrq_cnt;
				lc.rsrq_min = lc.rsrq_max;
			}
		}
	}

	private void SetPosi()
	{
		if (samprop.size() == 0)
		{
			return;
		}

		// UserLoca ul = new UserLoca();

		ul.GetLoca(samprop, cInfo, figs);

		FileSam(ul.samlocs);

		ul.Clear();

		samprop.clear();
	}

	private void FileSam(List<LocaData> ld)
	{
		if (ld.size() == 0)
		{
			return;
		}

		LOGHelper.GetLogger().writeLog(LogType.info, "start file ..." + String.valueOf(ld.size()));

		try
		{
			for (int ii = 0; ii < ld.size(); ii++)
			{
				LocaData sl = ld.get(ii);

				String skey = String.valueOf(sl.eci) + "_" + String.valueOf(sl.s1apid);

				if (!ecis.containsKey(skey))
				{
					continue;
				}

				List<MrSam> msl = ecis.get(skey);

				for (int jj = 0; jj < msl.size(); jj++)
				{
					MrSam ms = msl.get(jj);
					if (ms.itime > sl.splice_etime || ms.itime < sl.splice_btime)
					{
						continue;
					}

					if (ms.mall.tsc.longitude <= 0)
					{
						ms.mall.ibuildingID = sl.buildingid;
						ms.mall.iheight = (short) sl.ilevel;
						ms.mall.tsc.longitude = sl.longitude;
						ms.mall.tsc.latitude = sl.latitude;
						ms.mall.simuLongitude = sl.longitude;
						ms.mall.simuLatitude = sl.latitude;
						ms.mall.locSource = StaticConfig.LOCTYPE_LOW;
						if ((sl.doortype == 1) || (sl.doortype == 3))
						{
							ms.mall.testType = StaticConfig.TestType_CQT;
						}
						else
						{
							ms.mall.testType = StaticConfig.TestType_DT;
						}
						ms.mall.locType = "fp";
					}

					if (sl.doortype == 1 || sl.doortype == 3)
					{
						ms.mall.samState = StaticConfig.ACTTYPE_IN;
					}
					else if (sl.doortype == 2 || sl.doortype == 4)
					{
						ms.mall.samState = StaticConfig.ACTTYPE_OUT;
					}
				}
			}
		}
		catch (Exception ee)
		{
		}

		LOGHelper.GetLogger().writeLog(LogType.info, "game over");
	}
}
