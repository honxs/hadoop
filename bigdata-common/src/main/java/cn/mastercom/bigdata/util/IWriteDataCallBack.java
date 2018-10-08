package cn.mastercom.bigdata.util;

/**
 * 回调
 * 接收一个类型为T的数据实体，并将其吐出
 * @author Kwong
 *
 * @param <T>
 */
public interface IWriteDataCallBack<T> {

	/**
	 * @param t
	 * @throws Exception
	 */
	void write(T t) throws Exception;
}
