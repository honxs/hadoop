package cn.mastercom.bigdata.spark.evtstat;

import org.apache.spark.api.java.function.Function2;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mergestat.deal.MergeDataFactory;

public class ReduceByKeyFunc implements Function2<String, String, String>
{
	public int type1;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ReduceByKeyFunc(int type1) {
		this.type1 = type1;
	}

	@Override
	public String call(String values1, String values2) throws Exception {
		IMergeDataDo mergeDataObject1 = MergeDataFactory.GetInstance().getMergeDataObject(type1);
		mergeDataObject1.fillData(values1.split("\t", -1), 0);
		
		IMergeDataDo mergeDataObject2 = MergeDataFactory.GetInstance().getMergeDataObject(type1);
		mergeDataObject2.fillData(values2.split("\t", -1), 0);

		mergeDataObject1.mergeData(mergeDataObject2);
		return mergeDataObject1.getData();
	}
}


