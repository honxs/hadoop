package cn.mastercom.bigdata.util.hadoop.mapred;

import java.util.List;

import cn.mastercom.bigdata.util.IDataOutputer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * 使用hadoop context来吐出
 * @author Kwong
 */
public class ContextOutputWrapper<KEYOUT> implements IDataOutputer{

	private TaskInputOutputContext<?,?,KEYOUT,Text> context;

	private KeyGenerator<KEYOUT> keyGenerator;
	
	private Text outValue;
	
	public ContextOutputWrapper(TaskInputOutputContext<?,?,KEYOUT,Text> context, KeyGenerator<KEYOUT> keyGenerator){
		this.context = context;
		this.keyGenerator = keyGenerator;
		outValue = new Text();
	}
	
	@Override
	public int pushData(int dataType, String value) {
		outValue.set(value);
		try {
			context.write(keyGenerator.generate(value), outValue);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int pushData(int dataType, List<String> valueList) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}
	
	public interface KeyGenerator<KEYOUT>{
		
		KEYOUT generate(String value);
	}
}
