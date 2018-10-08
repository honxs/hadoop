package cn.mastercom.bigdata.locuser_v3;

import java.io.BufferedReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import cn.mastercom.bigdata.util.redis.JedisClusterFactory;
import cn.mastercom.bigdata.util.redis.RedisUtil;
import redis.clients.jedis.JedisCluster;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class CfgIndoor {
	private Map<Integer, CityIndoor> cfgcity = new HashMap<Integer, CityIndoor>();

	public void Clear() {
		cfgcity.clear();
	}

	public boolean Init(ReportProgress rptProgress) {
		// 动态加载
		rptProgress.writeLog(0, "cfgcity count = " + String.valueOf(cfgcity.size()));

		for (Entry<Integer, CityIndoor> hm : cfgcity.entrySet()) {
			rptProgress.writeLog(0, "cfgcity cityid = " + String.valueOf(hm.getKey()) + ", count = "
					+ String.valueOf(hm.getValue().cfgindoor.size()));
		}

		return true;
	}

	private boolean ReadStat(int cid) {
		if (cfgcity.containsKey(cid)) {
			return true;
		}
		CityIndoor cin = new CityIndoor();

		cfgcity.put(cid, cin);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2)) 
		{
			String redisConfig = MainModel.GetInstance().getAppConfig().getRedisConfig();
			int  redisTimeOut = MainModel.GetInstance().getAppConfig().getRedisTimeOut();
			String redisPassword = MainModel.GetInstance().getAppConfig().getRedisPassword();
			JedisCluster jc = JedisClusterFactory.getInstance().getJedisCluster(redisConfig, redisTimeOut, redisPassword);
			String rowKey = String.valueOf(cid)+ ".tb_cfg_indoor_building.txt";
			BufferedReader br =  RedisUtil.loadsFileFromRedis(jc, rowKey);
			return readIndoorConfig(br, rowKey, cin);
		}
		else
		{
			String fname = MainModel.GetInstance().getAppConfig().getSimuLocConfigPath() + "/" + String.valueOf(cid)
					+ "/indoor/tb_cfg_indoor_building.txt";
			CfgInfo.rProgress.writeLog(0, fname);
			BufferedReader sr = CfgInfo.getReader(fname, null);
			return readIndoorConfig(sr, fname, cin);
		}
	}

	
	private boolean readIndoorConfig(BufferedReader sr,String fname, CityIndoor cin) {
		if (sr == null) {
			CfgInfo.rProgress.writeLog(0, "open error : " + fname);
			return false;
		}
		try {
			int i = 0;
			String sline = sr.readLine();
			while (sline != null) {
				String[] recs = sline.split("\t", -1);
				if (recs.length < 3) {
					sline = sr.readLine();
					continue;
				}

				i = 0;
				try {
					int cityid = Integer.parseInt(recs[i++]);
					int buildingid = Integer.parseInt(recs[i++]);
					int eci = Integer.parseInt(recs[i++]);

					ArrayList<Integer> aa = null;
					if (!cin.cfgindoor.containsKey(buildingid)) {
						aa = new ArrayList<Integer>();
						cin.cfgindoor.put(buildingid, aa);
					} else {
						aa = cin.cfgindoor.get(buildingid);
					}
					aa.add(eci);

					ArrayList<Integer> bb = null;
					if (!cin.cfgineci.containsKey(eci)) {
						bb = new ArrayList<Integer>();
						cin.cfgineci.put(eci, bb);
					} else {
						bb = cin.cfgineci.get(eci);
					}
					bb.add(buildingid);
				} catch (Exception ee) {
					CfgInfo.rProgress.writeLog(0, ee.getMessage());
				}
				sline = sr.readLine();
			}
		} catch (Exception ee) {
			CfgInfo.rProgress.writeLog(0, ee.getMessage());
		}

		if (sr != null) {
			try {
				sr.close();
			} catch (IOException e) {
				CfgInfo.rProgress.writeLog(0, e.getMessage());
			}
		}
		return (cin.cfgindoor.size() > 0);
	}

	public List<Integer> GetIn(int cityid, int buildid) {
		if (!cfgcity.containsKey(cityid)) {
			ReadStat(cityid);
			if (!cfgcity.containsKey(cityid)) {
				return (new ArrayList<Integer>());
			}
		}

		CityIndoor cin = cfgcity.get(cityid);

		if (cin.cfgindoor.containsKey(buildid)) {
			return cin.cfgindoor.get(buildid);
		}
		return (new ArrayList<Integer>());
	}

	public int GetId(int cityid, int eci) {
		if (!cfgcity.containsKey(cityid)) {
			ReadStat(cityid);
			if (!cfgcity.containsKey(cityid)) {
				return -1;
			}
		}

		CityIndoor cin = cfgcity.get(cityid);

		if (cin.cfgineci.containsKey(eci)) {
			return cin.cfgineci.get(eci).get(0);
		}
		return -1;
	}
	

}
