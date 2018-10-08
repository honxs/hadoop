package cn.mastercom.bigdata.spark.evtstat;

import org.apache.spark.api.java.function.Function;

import cn.mastercom.bigdata.util.spark.TypeInfo;
import scala.Tuple2;

public class ResultFilterFunc implements  Function<Tuple2<TypeInfo,Iterable<String>>, Boolean>{

	@Override
	public Boolean call(Tuple2<TypeInfo, Iterable<String>> v1) throws Exception {
		return v1!=null;
	}
	
	
}
