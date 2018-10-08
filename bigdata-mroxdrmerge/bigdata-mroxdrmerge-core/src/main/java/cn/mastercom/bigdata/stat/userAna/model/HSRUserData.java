package cn.mastercom.bigdata.stat.userAna.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HSRUserData {
	//public Map<String, Set<HSRUserXdr>> userXdrMap;
	public List<HSRUserXdr> userXdrList;//输入的高铁用户xdr
    public List<HSRUserArea> userAreaList;
    
	public HSRUserData() {
		
		this.userXdrList = new ArrayList<HSRUserXdr>();
		this.userAreaList = new ArrayList<HSRUserArea>();
	}
	
}
