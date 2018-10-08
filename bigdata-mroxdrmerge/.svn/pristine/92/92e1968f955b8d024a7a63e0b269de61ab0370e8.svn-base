package cn.mastercom.bigdata.mro.stat;

import java.util.Collection;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;

public class RuBuildStatDo extends CompositeStatDo {

	public RuBuildStatDo(Collection<IStatDo> stats) {
		super(stats);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int stat(Object tsam) {
		
		if(((DT_Sample_4G)tsam).locType.contains("ru"))
			return super.stat(tsam);
		else{
			return 0;
		}
	}

}
