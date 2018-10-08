package cn.mastercom.bigdata.xdr.loc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LabelItemMng
{
    private List<LabelItem> labelItemList;
    
    public LabelItemMng()
    {
    	labelItemList = new ArrayList<LabelItem>();
    }
    
    public void AddItem(LabelItem item)
    {
    	labelItemList.add(item);
    }
    
    public List<LabelItem> getLabelItemList()
	{
		return labelItemList;
	}
    
    public void init()
    {	
		Collections.sort(labelItemList, new Comparator<LabelItem>()
		{
			public int compare(LabelItem a, LabelItem b)
			{
				return a.begin_time - b.begin_time;
			}
		});
    }
    
    public LabelItem getLabelItem(int time)
    {
    	for(LabelItem item : labelItemList)
    	{
    		if(time >= item.begin_time && time <= item.end_time)
    		{
    			return item;
    		}
    		
            if(time < item.begin_time)
            {
            	return null;
            }
    	}
    	return null;
    }
    
}
