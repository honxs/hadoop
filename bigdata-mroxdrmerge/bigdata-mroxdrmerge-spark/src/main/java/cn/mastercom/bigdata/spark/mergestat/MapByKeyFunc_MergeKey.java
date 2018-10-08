package cn.mastercom.bigdata.spark.mergestat;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mergestat.deal.MergeDataFactory;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class MapByKeyFunc_MergeKey implements PairFunction<String, MergeTypeItem, String>
{
	private static final long serialVersionUID = 1L;
	
	private MergeTypeInfo typeInfo = null;
    private int type = -1;
    private IMergeDataDo mergeItem;

	public MapByKeyFunc_MergeKey(int type)  throws Exception
	{
		this.type = type;
		mergeItem =  MergeDataFactory.GetInstance().getMergeDataObject(type);
	}

	@Override
	public Tuple2<MergeTypeItem, String> call(String line) throws Exception {
		mergeItem.fillData(line.split("\t"), 0);

		String key = mergeItem.getMapKey();
		MergeTypeItem item = new MergeTypeItem(key, type);

		return new Tuple2<>(item, line);
	}
}
