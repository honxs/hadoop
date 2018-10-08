package cn.mastercom.bigdata.standalone.stat.pha;

import cn.mastercom.bigdata.standalone.main.StartUp;

public class PhaMain
{
	public static void main(String[] args)
	{
		StartUp startUp = new StartUp(new PhaWork());
		startUp.start();
	}
}
