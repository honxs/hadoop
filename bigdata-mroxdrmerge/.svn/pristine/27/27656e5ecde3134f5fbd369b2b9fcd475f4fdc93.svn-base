package com.chinamobile.util;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class StringFormat {

	public static String format(String format, Object... values){
		String expression=format;
		Pattern formatPattern = Pattern.compile("\\{\\s*(\\d+)\\s*\\}");
		Matcher paraMatcher = formatPattern.matcher(expression);
		StringBuffer paraStrBuf = new StringBuffer();
		int i=0;
		while (paraMatcher.find()) {
			if(i >= values.length){
				break;
			}
			Object curObject = values[i++];
			String paraValue = "";
			if(curObject!=null){
				 paraValue = curObject.toString();
			}
			paraMatcher.appendReplacement(paraStrBuf, paraValue);
		}
		paraMatcher.appendTail(paraStrBuf);
		expression = paraStrBuf.toString();
		return expression;

	}
	
	
	public static String format(String format, Map<String,Object> valueMap){
		String expression = format;
		Pattern formatPattern = Pattern.compile("\\{\\s*(\\w+)\\s*\\}");
		Matcher paraMatcher = formatPattern.matcher(expression);
		StringBuffer paraStrBuf = new StringBuffer();
		while (paraMatcher.find()) {
			String key=paraMatcher.group(1);
			String paraValue = "";
			if(valueMap!=null&&valueMap.containsKey(key)){
				 paraValue = valueMap.get(key).toString();
				 paraMatcher.appendReplacement(paraStrBuf, paraValue);
			}
		}
		paraMatcher.appendTail(paraStrBuf);
		expression = paraStrBuf.toString();
		return expression;
	}

	
	public static void main(String[] args) {
		System.out.println(StringFormat.format("[{ 'text': '{0}', 'expanded': {1} }]", "funck", true));
		 Map<String,Object> valueMap = new HashMap<String, Object>();
		 valueMap.put("table", "GX");
		 valueMap.put("task", 11);
		System.out.println(StringFormat.format(
				"nbrmr.F_G_C_MR_TASK_NBR_{table} and TASK_ID = {task},nbrmr.F_G_C_MR_TASK_NBR_{table} and TASK_ID = {task}",
				valueMap)
			);
	}
}
