package cn.mastercom.bigdata.util;

import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.ItemLabelAnchor;
import org.jfree.chart.labels.ItemLabelPosition;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.IntervalMarker;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.Day;
import org.jfree.data.time.Hour;
import org.jfree.data.time.Minute;
import org.jfree.data.time.Month;
import org.jfree.data.time.RegularTimePeriod;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.Year;
import org.jfree.ui.Layer;
import org.jfree.ui.LengthAdjustmentType;
import org.jfree.ui.TextAnchor;

public final class FreeChartUtil {
	
	public enum ChartType{
		TIME_SERIES, LINE, PIE
	}
	
	public interface IChart{
		
		void print(String path) throws IOException ;
		
		void show();
	}
	
	public interface IChartValue{
		
	}
	
	public interface IChartValueGroup<T extends IChartValue>{
		
		String getGroupName();
		
		Collection<T> getGroupData();
	}
	
	public static class TimeSeriesChart implements IChart{
		
		JFreeChart chart;
		
		public enum Time{
			SECOND(Second.class), MINUTE(Minute.class), HOUR(Hour.class), DAY(Day.class), MONTH(Month.class), YEAR(Year.class);
			
			Class<? extends RegularTimePeriod> freeChartTimeClass;
			
			Time(Class<? extends RegularTimePeriod> freeChartTimeClass){
				this.freeChartTimeClass = freeChartTimeClass;
			}
			
			public Class<? extends RegularTimePeriod> getFreeChartTimeClass(){
				return freeChartTimeClass;
			}
		}

		TimeSeriesChart(JFreeChart chart) {
			this.chart = chart;
		}
		
		@Override
		public void print(String path) throws IOException {
			File fos_jpg = new File(path);
			// 输出到哪个输出流
			ChartUtilities.saveChartAsJPEG(fos_jpg, chart, // 统计图表对象
					1200, // 宽
					700 // 高
					);
		}

		@Override
		public void show() {
			ChartFrame mChartFrame = new ChartFrame("折线图", chart);
		    mChartFrame.pack();
		    mChartFrame.setVisible(true);
		}
	}
	
	public static abstract class TimeSeriesChartValue implements IChartValue{
		
		public abstract Date getDate();
		
		public abstract double getValue();
	}
	
	public static abstract class TimeSeriesChartLine<T extends TimeSeriesChartValue> implements IChartValueGroup<T>{
		
		protected String seriesName;
		
		protected Collection<T> values;
		
		public TimeSeriesChartLine(){
			values = new ArrayList<>();
		}
		
		public String getGroupName(){
			return seriesName;
		}
		
		public Collection<T> getGroupData(){
			return values;
		}
		
		public void add(T value){
			values.add(value);
		}
	}
	
	private static DefaultCategoryDataset createCateforyDataset(List<IChartValue> values){
		return null;
	}
	
	public static TimeSeriesChart createTimeSeriesChart(String title, String xLable ,String yLable, TimeSeriesChart.Time timeType, List<? extends TimeSeriesChartLine<?>> lines){
		
		TimeSeriesCollection timeSeriesCollection = null;
		try {
			timeSeriesCollection = createTimeSeriesDataset(timeType.getFreeChartTimeClass(), lines);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("数据集生成失败", e);
		}
		
		JFreeChart chart = ChartFactory.createTimeSeriesChart(title, xLable, yLable, timeSeriesCollection, true, true, true);
	
		return new TimeSeriesChart(chart);
	}
	
	@SuppressWarnings({ "rawtypes", "deprecation", "unchecked" })
	private static TimeSeriesCollection createTimeSeriesDataset(Class<? extends RegularTimePeriod> clazz, List<? extends TimeSeriesChartLine<?>> lines) throws Exception{
		
		assert clazz.isAssignableFrom(RegularTimePeriod.class);
		Constructor<? extends RegularTimePeriod> cons = clazz.getConstructor(Date.class);
		
		TimeSeriesCollection result = new TimeSeriesCollection();
		
		for(TimeSeriesChartLine line : lines){			
			
			TimeSeries s = new TimeSeries(line.getGroupName(), clazz);
			
			
			Collection<TimeSeriesChartValue> values = line.getGroupData();
			
			for(TimeSeriesChartValue value : values){
				s.add(cons.newInstance(value.getDate()), value.getValue());
			}
			
			result.addSeries(s);
		}
		
		return result;
	}
	
	
	public static void setChartTheme(){
		//创建主题样式  
		   StandardChartTheme standardChartTheme=new StandardChartTheme("CN");  
		   //设置标题字体  
		   standardChartTheme.setExtraLargeFont(new Font("隶书",Font.BOLD,20));  
		   //设置图例的字体  
		   standardChartTheme.setRegularFont(new Font("宋书",Font.PLAIN,15));  
		   //设置轴向的字体  
		   standardChartTheme.setLargeFont(new Font("宋书",Font.PLAIN,15));  
		   //应用主题样式  
		   ChartFactory.setChartTheme(standardChartTheme); 
	}
	
	
}



class test {

	public test() {
		this.createChart();
	}
	
	
	// 获得数据集 （这里的数据是为了测试我随便写的一个自动生成数据的例子）
	public DefaultCategoryDataset createDataset() {

		DefaultCategoryDataset linedataset = new DefaultCategoryDataset();
		// 曲线名称
		String series = "1711271800051001"; // series指的就是报表里的那条数据线
		// 因此 对数据线的相关设置就需要联系到serise
		// 比如说setSeriesPaint 设置数据线的颜色

		// 横轴名称(列名称)
		// 随机添加数据值
//		int startTime = 1511777067;
		Integer[] timeLine = new Integer[]{1511777155,1511777167,1511777223,1511777305,1511777323,1511777350,1511777366,1511777388,1511777412,1511777429,1511777444,1511777467};
		Double[] dists = new Double[]{1918.5,2363.0,5106.0,6562.833333333333,7291.25,8687.833333333334,8714.4,10552.5,11698.714285714286,12791.888888888889,13769.0,14358.666666666666};
		for (int i = 0; i < timeLine.length; i++) {
			linedataset.addValue(dists[i], // 值
					series, // 哪条数据线
					/*Integer.valueOf(timeLine[i] - startTime)*/new Date(timeLine[i] * 1000)); // 对应的横轴
		}

		return linedataset;
	}

	// 生成图标对象JFreeChart
	/*
	 * 整个大的框架属于JFreeChart坐标轴里的属于 Plot 其常用子类有：CategoryPlot, MultiplePiePlot,
	 * PiePlot , XYPlot
	 * 
	 * **
	 */
	public void createChart() {

		try {
			// 定义图标对象
			JFreeChart chart = ChartFactory.createLineChart(null,// 报表题目，字符串类型
					"时间", // 横轴
					"离始发站距离", // 纵轴
					this.createDataset(), // 获得数据集
					PlotOrientation.VERTICAL, // 图标方向垂直
					true, // 显示图例
					true, // 不用生成工具
					false // 不用生成URL地址
					);
			// 整个大的框架属于chart 可以设置chart的背景颜色

			// 生成图形
			CategoryPlot plot = chart.getCategoryPlot();
				
			// 图像属性部分
			plot.setBackgroundPaint(Color.white);
			plot.setDomainGridlinesVisible(true); // 设置背景网格线是否可见
			plot.setDomainGridlinePaint(Color.BLACK); // 设置背景网格线颜色
			plot.setRangeGridlinePaint(Color.GRAY);
			plot.setNoDataMessage("没有数据");// 没有数据时显示的文字说明。
			
			// 数据轴属性部分
			NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
			rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
			rangeAxis.setAutoRangeIncludesZero(true); // 自动生成
			rangeAxis.setUpperMargin(0.20);
			rangeAxis.setLabelAngle(Math.PI / 2.0);
			rangeAxis.setAutoRange(false);
			
			
			
			// 数据渲染部分 主要是对折线做操作
			LineAndShapeRenderer renderer = (LineAndShapeRenderer) plot
					.getRenderer();
			renderer.setBaseItemLabelsVisible(true);
			renderer.setSeriesPaint(0, Color.black); // 设置折线的颜色
			renderer.setBaseShapesFilled(true);
			renderer.setBaseItemLabelsVisible(true);
			renderer.setBasePositiveItemLabelPosition(new ItemLabelPosition(
					ItemLabelAnchor.OUTSIDE12, TextAnchor.BASELINE_LEFT));
			renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
			/*
			 * 这里的StandardCategoryItemLabelGenerator()我想强调下：当时这个地*方被搅得头很晕，Standard
			 * **ItemLabelGenerator是通用的 因为我创建*的是CategoryPlot 所以很多设置都是Category相关
			 * 而XYPlot 对应的则是 ： StandardXYItemLabelGenerator
			 */
			// 对于编程人员 这种根据一种类型方法联想到其他类型相似方法的思
			// 想是必须有的吧！目前只能慢慢培养了。。
			renderer.setBaseItemLabelFont(new Font("Dialog", 1, 14)); // 设置提示折点数据形状
			plot.setRenderer(renderer);
			// 区域渲染部分
			double lowpress = 4.5;
			double uperpress = 80; // 设定正常血糖值的范围
			IntervalMarker inter = new IntervalMarker(lowpress, uperpress);
			inter.setLabelOffsetType(LengthAdjustmentType.EXPAND); // 范围调整——扩张
			inter.setPaint(Color.LIGHT_GRAY);// 域顏色

			inter.setLabelFont(new Font("SansSerif", 41, 14));
			inter.setLabelPaint(Color.RED);
			inter.setLabel("正常血糖值范围"); // 设定区域说明文字
			plot.addRangeMarker(inter, Layer.BACKGROUND); // 添加mark到图形
															// BACKGROUND使得数据折线在区域的前端
		/*	
			// 创建文件输出流
			File fos_jpg = new File("E://bloodSugarChart.jpg ");
			// 输出到哪个输出流
			ChartUtilities.saveChartAsJPEG(fos_jpg, chart, // 统计图表对象
					700, // 宽
					500 // 高
					);
*/
			ChartFrame mChartFrame = new ChartFrame("折线图", chart);
		    mChartFrame.pack();
		    mChartFrame.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	// 测试类
	/*public static void main(String[] args) {
		System.out.println("-----------");
		
		setChartTheme();
//		test my = new test();
		
		ChartFrame mChartFrame = new ChartFrame("折线图", FreeChartUtil.createChart(null, null));
	    mChartFrame.pack();
	    mChartFrame.setVisible(true);
	}
*/
	
}