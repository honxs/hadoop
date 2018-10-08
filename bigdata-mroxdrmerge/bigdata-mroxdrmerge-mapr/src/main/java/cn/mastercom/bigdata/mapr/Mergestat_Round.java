package cn.mastercom.bigdata.mapr;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;

import org.apache.hadoop.conf.Configuration;

public class Mergestat_Round
{
	public static void doMergestat4(String queueName, String statTime, String mroXdrMergePath, Configuration conf, HDFSOper hdfsOper)
	{
		// // 执行xdr运算
		// int pcnt = 0;
		// String[] myArgs = new String[1000];
		// myArgs[pcnt++] = "100";
		// myArgs[pcnt++] = queueName;
		// myArgs[pcnt++] = statTime;
		// myArgs[pcnt++] = String.format("%s/mergestat4/data_%s",
		// mroXdrMergePath, statTime);
		//
		// myArgs[pcnt++] = "40";// input path count
		// //mro round grid bigger 2017.9.12
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_OUTGRID_HIGH;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_OUTGRID_MID;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_OUTGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_OUTGRID_LOW;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_OUTGRID_CELL_HIGH;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_OUTGRID_CELL_MID;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_OUTGRID_CELL_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_OUTGRID_CELL_LOW;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_INGRID_HIGH;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_INGRID_MID;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_INGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_INGRID_LOW;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_INGRID_CELL_HIGH;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_INGRID_CELL_MID;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_INGRID_CELL_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_INGRID_CELL_LOW;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_INGRID_CELL_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_OUTGRID_HIGH;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_OUTGRID_MID;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_OUTGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_OUTGRID_LOW;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_INGRID_HIGH;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_INGRID_MID;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_INGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_INGRID_LOW;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_OUTGRID_HIGH;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_OUTGRID_MID;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_OUTGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_OUTGRID_LOW;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_INGRID_HIGH;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_INGRID_MID;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_INGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_INGRID_LOW;
		// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// // mdt round grid bigger 2017.9.12
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_YD_OUTGRID_HIGH;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_YD_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_YD_OUTGRID_LOW;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_YD_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_YD_INGRID_HIGH;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_YD_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_YD_INGRID_LOW;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_YD_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_INGRID_CELL_HIGH;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_INGRID_CELL_LOW;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_INGRID_CELL_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_OUTGRID_CELL_HIGH;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_OUTGRID_CELL_LOW;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_LT_OUTGRID_HIGH;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_LT_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_LT_OUTGRID_LOW;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_LT_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_LT_INGRID_HIGH;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_LT_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_LT_INGRID_LOW;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_LT_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_DX_OUTGRID_HIGH;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_DX_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_DX_OUTGRID_LOW;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_DX_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_DX_INGRID_HIGH;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_DX_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_DX_INGRID_LOW;
		//// myArgs[pcnt++] = String.format("%1$s/mergestat3/data_01_%2$s" +
		// MdtNewTableStat.MDT_DX_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		//
		// // output
		// myArgs[pcnt++] = "40";// output path count
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_OUTGRID_HIGH;
		// myArgs[pcnt++] = String.format("mrhighoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_OUTGRID_MID;
		// myArgs[pcnt++] = String.format("mrmidoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_OUTGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_OUTGRID_LOW;
		// myArgs[pcnt++] = String.format("mrlowoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_OUTGRID_CELL_HIGH;
		// myArgs[pcnt++] = String.format("mrhighoutgridcell");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_OUTGRID_CELL_MID;
		// myArgs[pcnt++] = String.format("mrmidoutgridcell");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_OUTGRID_CELL_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_OUTGRID_CELL_LOW;
		// myArgs[pcnt++] = String.format("mrlowoutgridcell");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_INGRID_HIGH;
		// myArgs[pcnt++] = String.format("mrhighingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_INGRID_MID;
		// myArgs[pcnt++] = String.format("mrmidingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_INGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_YD_INGRID_LOW;
		// myArgs[pcnt++] = String.format("mrlowingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_YD_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_INGRID_CELL_HIGH;
		// myArgs[pcnt++] = String.format("mrhighingridcell");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_INGRID_CELL_MID;
		// myArgs[pcnt++] = String.format("mrmidingridcell");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_INGRID_CELL_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_INGRID_CELL_LOW;
		// myArgs[pcnt++] = String.format("mrlowingridcell");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_INGRID_CELL_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_OUTGRID_HIGH;
		// myArgs[pcnt++] = String.format("mrdxhighoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_OUTGRID_MID;
		// myArgs[pcnt++] = String.format("mrdxmidoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_OUTGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_OUTGRID_LOW;
		// myArgs[pcnt++] = String.format("mrdxlowoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_INGRID_HIGH;
		// myArgs[pcnt++] = String.format("mrdxhighingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_INGRID_MID;
		// myArgs[pcnt++] = String.format("mrdxmidingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_INGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_DX_INGRID_LOW;
		// myArgs[pcnt++] = String.format("mrdxlowingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_DX_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_OUTGRID_HIGH;
		// myArgs[pcnt++] = String.format("mrlthighoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_OUTGRID_MID;
		// myArgs[pcnt++] = String.format("mrltmidoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_OUTGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_OUTGRID_LOW;
		// myArgs[pcnt++] = String.format("mrltlowoutgrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_INGRID_HIGH;
		// myArgs[pcnt++] = String.format("mrlthighingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_INGRID_MID;
		// myArgs[pcnt++] = String.format("mrltmidingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_INGRID_MID + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// myArgs[pcnt++] = "" + MergeDataFactory.MRO_ROUND_LT_INGRID_LOW;
		// myArgs[pcnt++] = String.format("mrltlowingrid");
		// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MroTableRegister_BS.MRO_LT_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		//// // mdt round grid bigger 2017.9.12
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_YD_OUTGRID_HIGH;
		//// myArgs[pcnt++] = String.format("mdtoutgridhigh");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_YD_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_YD_OUTGRID_LOW;
		//// myArgs[pcnt++] = String.format("mdtoutgridlow");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_YD_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_YD_INGRID_HIGH;
		//// myArgs[pcnt++] = String.format("mdtingridhigh");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_YD_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_YD_INGRID_LOW;
		//// myArgs[pcnt++] = String.format("mdtingridlow");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_YD_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_INGRID_CELL_HIGH;
		//// myArgs[pcnt++] = String.format("mdtingridcellhigh");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_INGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_INGRID_CELL_LOW;
		//// myArgs[pcnt++] = String.format("mdtingridcelllow");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_INGRID_CELL_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_OUTGRID_CELL_HIGH;
		//// myArgs[pcnt++] = String.format("mdtoutgridcellhigh");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_OUTGRID_CELL_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_OUTGRID_CELL_LOW;
		//// myArgs[pcnt++] = String.format("mdtoutgridcelllow");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_OUTGRID_CELL_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_LT_OUTGRID_HIGH;
		//// myArgs[pcnt++] = String.format("ltmdtoutgridhigh");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_LT_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_LT_OUTGRID_LOW;
		//// myArgs[pcnt++] = String.format("ltmdtoutgridlow");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_LT_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_LT_INGRID_HIGH;
		//// myArgs[pcnt++] = String.format("ltmdtingridhigh");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_LT_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_LT_INGRID_LOW;
		//// myArgs[pcnt++] = String.format("ltmdtingridlow");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_LT_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_DX_OUTGRID_HIGH;
		//// myArgs[pcnt++] = String.format("dxmdtoutgridhigh");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_DX_OUTGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_DX_OUTGRID_LOW;
		//// myArgs[pcnt++] = String.format("dxmdtoutgridlow");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_DX_OUTGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_DX_INGRID_HIGH;
		//// myArgs[pcnt++] = String.format("dxmdtingirdhigh");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_DX_INGRID_HIGH + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		////
		//// myArgs[pcnt++] = "" + MergeDataFactory.MDT_ROUND_DX_INGRID_LOW;
		//// myArgs[pcnt++] = String.format("dxmdtingirdlow");
		//// myArgs[pcnt++] = String.format("%1$s/mergestat4/data_01_%2$s" +
		// MdtNewTableStat.MDT_DX_INGRID_LOW + "%2$s", mroXdrMergePath,
		// statTime.replace("01_", ""));
		//
		// String[] params = new String[pcnt];
		// for (int i = 0; i < pcnt; ++i)
		// {
		// params[i] = myArgs[i];
		// }
		//
		// try
		// {
		// String _successFile = myArgs[3] + "/output/_SUCCESS";
		// if (mroXdrMergePath.contains(":") ||
		// !hdfsOper.checkFileExist(_successFile))
		// {
		// Job curJob = MergeStatMain.CreateJob(conf, params);
		//
		// DataDealJob dataJob = new DataDealJob(curJob, hdfsOper);
		// if (!dataJob.Work())
		// {
		// System.out.println("mergestat job error! stop run.");
		// throw (new Exception("system.exit1"));
		// }
		// }
		// else
		// {
		// System.out.println("MERGESTAT3 has been dealed succesfully:" +
		// myArgs[3]);
		// }
		// }
		// catch (Exception e)
		// {
		// }
	}
}
