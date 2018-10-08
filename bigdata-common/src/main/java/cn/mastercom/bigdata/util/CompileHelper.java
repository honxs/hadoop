package cn.mastercom.bigdata.util;

import java.util.HashMap;
import java.util.Map;

public class CompileHelper
{
	private boolean[] marks;
	private Map<String, Integer> markMap;
	
	public CompileHelper()
	{
		clear();
	}
	
	public boolean[] getMarks(){
		return marks;
	}
	
	private void clear()
	{
		marks = new boolean[1000];
		for(int i=0; i<marks.length; ++i)
		{
			marks[i] = false;
		}
		
		markMap = new HashMap<String, Integer>();
	}

	public boolean Assert(int mark)
	{
		if (mark < marks.length && marks[mark] == true)
		{
			return true;
		}

		return false;
	}
	
	public boolean initFormatMark(String formatMarksStr)
	{
		clear();
		
		String[] strs = formatMarksStr.split(",");
		for (int i=0; i<strs.length; ++i)
		{
			marks[i+1] = false;
			markMap.put(strs[i].toUpperCase(), i+1);
		}
		return true;
	}
	
	public boolean initMark(String marksStr)
	{		
		String[] strs = marksStr.split(",");
		for (int i=0; i<strs.length; ++i)
		{
			String markStr = strs[i].toUpperCase().trim();
			if(markMap.containsKey(markStr))
			{
				int index = markMap.get(markStr);
				marks[index] = true;
			}
		}
		
		return true;
	}

}
