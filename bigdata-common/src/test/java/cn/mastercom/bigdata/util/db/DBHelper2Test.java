package cn.mastercom.bigdata.util.db;

import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.Ignore;
import org.junit.Test;


public class DBHelper2Test {

	@Ignore
	@Test
	public void test() throws SQLException {
		DBHelper2 pGHelper = DBHelper2.builder()
//				.confFile("/zmk/wlyh2/PostgreSQL.jdbc.properties")	// jdbc配置
//				.sqlFile("/zmk/wlyh2/sql.xml")	// 外部sql.xml地址
				.username("dtauser")
				.password("dtauser")
				.url("jdbc:postgresql://192.168.1.66:5432/HAERBIN")
				.driver("org.postgresql.Driver")
				.build();
		pGHelper.setKey("postgre");	// 保存到DBHelper2下的一个静态HashMap里
		DBHelper2 sSHelper = DBHelper2.builder()
//				.confFile("/zmk/wlyh2/SQLServer.jdbc.properties")	// jdbc配置
//				.sqlFile("/zmk/wlyh2/sql.xml")	// 外部sql.xml地址
				.username("dtauser")
				.password("dtauser")
				.url("jdbc:sqlserver://192.168.1.50;instanceName=newdb")
				.driver("com.microsoft.sqlserver.jdbc.SQLServerDriver")
				.build();
		sSHelper.setKey("sqlserver");
		
		// 第一种查询方式
		pGHelper.getRunner().query("sql 语句", new MapListHandler());
		
		// 第二种查询方式
		sSHelper.query("sql.xml 配置中对应的sql语句id", new MapListHandler(), "参数1", "参数2", "参数n");
		
		// 增、改、删需要调用runner里的方法
		QueryRunner pGRunner = pGHelper.getRunner();
		pGRunner.update("增、改、删的 sql 语句");

	}

}
