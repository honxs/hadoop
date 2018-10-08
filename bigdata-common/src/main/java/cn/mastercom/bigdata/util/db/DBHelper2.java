package cn.mastercom.bigdata.util.db;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * 测试类为{@link cn.mastercom.bigdata.util.db.DBHelper2Test} 
 *
 */
public class DBHelper2  {
	
	static Logger log = Logger.getLogger(DBHelper2.class.getName());
	
	private static Map<String, DBHelper2> DBHelpers = new HashMap<>();

	/**
	 * 数据库驱动
	 */
	private String driver;
	
	/**
	 * 数据库用户名
	 */
	private String username;
	
	/**
	 * 数据库密码
	 */
	private String password;
	
	/**
	 * 数据库链接
	 */
	private String url;

	/**
	 * 数据库配置文件，必须是<tt>.properties</tt>文件，格式要求：
	 * <p>
	 * <code>
	 * jdbc.username=xxxx
	 * <br>
	 * jdbc.password=xxxx
	 * <br>
	 * jdbc.url=xxxx
	 * <br>
	 * jdbc.driver=xxxx
	 * </code>
	 * <p>
	 * 如果配置此项，则可不必再重复配置相关字段
	 */
	private String confFile;
	
	/**
	 * 外部sql文件，必须为<tt>.xml</tt>文件，格式要求：
	 * <p>
	 * <code>
	 * &lt;sql&gt;
	 * <br>
	 * &nbsp;&lt;select id="xxx"&gt;
	 * <br>
	 * &nbsp;&nbsp;select语句
	 * <br>
	 * &nbsp;&lt;/select&gt;
	 * <br>
	 * &nbsp;....
	 * <br>
	 * &nbsp;&lt;insert id="xxx"&gt;
	 * <br>
	 * &nbsp;&nbsp;insert语句
	 * <br>
	 * &nbsp;&lt;/insert&gt;
	 * <br>
	 * &nbsp;....
	 * <br>
	 * &lt;/sql&gt;
	 * </code>
	 */
	private String sqlFile;
	private QueryRunner queryRunner;
	
	private Map<String, String> sqlMap;
	
	/**
	 * 判断是否已经建造，如果还未建造，不能调用与查询相关的方法
	 */
	private boolean build = false;
	
	/**
	 * 判断传入的sql文件是否有效，如果无效则不能调用与sql文件相关的查询方法
	 */
	private boolean validSqlFile = false;
	
	/**
	 * 判断本对象是否已加入{@link #DBHelpers}中
	 */
	private boolean added = false;
	
	/**
	 * 不允许外部来使用构造方法
	 */
	private DBHelper2() {super();}
	
	/**
	 * 读取外部sql.xml文件
	 */
	private void readSQLXml(File file) {
		SAXReader reader = new SAXReader();
	
		if (file.exists()) {
			try {
				Document doc = reader.read(file);
				Element root = doc.getRootElement();
				List<?> elements = root.elements();
				for (int i = 0; i < elements.size(); i++) {
					Element e = (Element)elements.get(i);
					Attribute idAttr = e.attribute("id");
					String id =idAttr.getValue();
					String value = e.getText();
					sqlMap.put(id, value);
				}
			} catch (DocumentException e) {log.error(e);}
		}
	}
	
	/**
	 * 设置数据库驱动
	 * @param driver
	 * @return
	 */
	public DBHelper2 driver(String driver) {
		this.driver = driver;
		return this;
	}
	
	/**
	 * 设置数据库用户名
	 * @param username
	 * @return
	 */
	public DBHelper2 username(String username) {
		this.username = username;
		return this;
	}
	
	/**
	 * 设置数据库用户密码
	 * @param password
	 * @return
	 */
	public DBHelper2 password(String password) {
		this.password = password;
		return this;
	}
	
	/**
	 * 设置数据库链接
	 * @param url
	 * @return
	 */
	public DBHelper2 url(String url) {
		this.url = url;
		return this;
	}
	
	/**
	 * 设置数据的配置文件，设置了此配置文件之后就不用再重复设置用户名、密码、驱动和链接了
	 * @param filePath 以类路径为根路径
	 * @return
	 */
	public DBHelper2 confFile(String filePath) {
		this.confFile = filePath;
		return this;
	}
	
	/**
	 * 外部sql文件的路径，必须是<tt>.xml</tt>文件
	 * @param filePath 以类路径为根路径
	 * @return
	 */
	public DBHelper2 sqlFile(String filePath) {
		this.sqlFile = filePath;
		return this;
	}
	
	/**
	 * 将本对象作为一个value放入一个HashMap中
	 * <p>
	 * 必须调用了{@link #build()}方法之后才能调用此方法
	 * @param aKey 本对象对应的key，因为HashMap的key可以为<tt>null</tt>，也可以为空字符串，因此，对于key并没有限制
	 */
	public void setKey(String aKey) {
		// 不能重复添加
		if (added)
			throw new RuntimeException("不能重复添加该对象");
		// 不能设置已有的key
		if (DBHelpers.get(aKey)!=null)
			throw new RuntimeException(aKey + "已经被占用，请尝试其他的值");
		// 只有该对象未加入DBHelpers中才能加入
		// 只有该对象已经build完成之后才能加入
		if (!added && build) {
			DBHelpers.put(aKey, this);
			added = true;
		}
	}
	
	/**
	 * 该方法会根据配置信息创建JDBC的上下文环境，同时会读取外部sql文件进入内存里。因此，在加载新的sql文件的时候需要再调用一次此方法。
	 * @return
	 */
	public DBHelper2 build() {
		if (queryRunner==null) {
			String driver = null;
			String username = null;
			String password = null;
			String url = null;
			// 先尝试从外部配置文件读取配置
			if (confFile!=null) {
				InputStream is = Object.class.getResourceAsStream(confFile);
				Properties p = new Properties();
				try {
					p.load(is);
					driver = p.getProperty("jdbc.driver");
					username = p.getProperty("jdbc.username");
					password = p.getProperty("jdbc.password");
					url = p.getProperty("jdbc.url");
				} catch (IOException ignored) {
					log.error(ignored);
				}
				
			}
			// 再从字段读取文件配置
			if (this.driver != null)
				driver = this.driver;
			else
				this.driver = driver;
			if (this.username != null)
				username = this.username;
			else
				this.username = username;
			if (this.password != null)
				password = this.password;
			else
				this.password = password;
			if (this.url != null)
				url = this.url;
			else
				this.url = url;
			
			// 判断是否已经创建
			if (DBHelpers.size()>0) {
				Set<Entry<String,DBHelper2>> entrySet = DBHelpers.entrySet();
				for (Map.Entry<String, DBHelper2> entry : entrySet) {
					if (entry.getValue().hashCode()==this.hashCode()) {
						// 不允许重复创建
						throw new RuntimeException("你已创建过同样的实例并保存在DBHelpers里了，请通过DBHelper.get(String)方法去获取该实例，不要再重复创建！");
					}
				}
			}
			
			// 没创建过才能创建
			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setDriverClassName(driver);
			dataSource.setUsername(username);
			dataSource.setPassword(password);
			dataSource.setUrl(url);
			queryRunner = new QueryRunner(dataSource);
			
			build = true;
		}
		// 尝试着加载外部sql文件
		if (sqlFile!=null) {
			String filePath = Object.class.getResource(sqlFile).getFile();
			File file = new File(filePath);
			if (file!=null&&file.exists()) {
				sqlMap = new HashMap<>();
				readSQLXml(file);
				validSqlFile = true;
			}
		}
		return this;
	}
	
	/**
	 * 根据配置的数据库信息获取apache commons-DBUtils的QueryRunner
	 * <p>
	 * 该类提供一系列数据增删改查的方法
	 * @return
	 */
	public QueryRunner getRunner () {
		if (build)
			return queryRunner;
		throw new RuntimeException("你必须先调用build()方法，之后才能调用此方法");
	}

	/**
	 * 根据外部sql文件配置的sqlId获取sql语句然后执行sql语句
	 * @param sqlId sql文件配置的id
	 * @param rsh 根据需要传入的结果集处理器
	 * @param args sql语句里的占位符对应的参数
	 * @return 根据结果集的不同有不同的返回值
	 */
	public <T>T query(String sqlId, ResultSetHandler<T> rsh, Object...args) {
		if (!validSqlFile) throw new RuntimeException("你并没有传入外部sql文件的路径或你传入的路径不正确或你传入的sql文件不规范，请重新仔细检查");
		
		if (sqlId==null||sqlId.length()==0) return null;
		
		String sql = sqlMap.get(sqlId);
		
		try {
			if (args==null||args.length==0)
				return getRunner().query(sql, rsh);
			else
				return getRunner().query(sql, rsh, args);
		} catch (SQLException ignored) {
			log.error(ignored);
			return null;
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((driver == null) ? 0 : driver.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		result = prime * result + ((username == null) ? 0 : username.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DBHelper2 other = (DBHelper2) obj;
		if (driver == null) {
			if (other.driver != null)
				return false;
		} else if (!driver.equals(other.driver))
			return false;
		if (password == null) {
			if (other.password != null)
				return false;
		} else if (!password.equals(other.password))
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		if (username == null) {
			if (other.username != null)
				return false;
		} else if (!username.equals(other.username))
			return false;
		return true;
	}

	/**
	 * 此类唯一的构造方法
	 * @return
	 */
	public static DBHelper2 builder() {
		return new DBHelper2();
	}

	/**
	 * 根据key从{@link #DBHelpers}中获取本对象
	 * @param aKey
	 * @return
	 */
	public static DBHelper2 get(String aKey) {
		return DBHelpers.get(aKey);
	}
	
}
