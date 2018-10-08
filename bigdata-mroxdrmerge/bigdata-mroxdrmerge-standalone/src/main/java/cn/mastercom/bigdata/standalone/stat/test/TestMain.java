package cn.mastercom.bigdata.standalone.stat.test;

import cn.mastercom.bigdata.standalone.main.StartUp;

public class TestMain
{
	public static void main(String[] args)
	{
		StartUp startUp = new StartUp(new TestWork());
		startUp.start();
	}
}
