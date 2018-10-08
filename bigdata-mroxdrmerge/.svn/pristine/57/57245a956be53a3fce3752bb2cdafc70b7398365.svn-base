package cn.mastercom.bigdata.evt.locall.stat;

public class EventDataStatKey
{
    private int Interface;
    private int kpiset;
    private int ProcedureType;
    
    public EventDataStatKey(int Interface, int kpiset, int ProcedureType)
    {
    	this.Interface = Interface;
    	this.kpiset = kpiset;
    	this.ProcedureType = ProcedureType;
    }
    
    public int getInterface()
	{
		return Interface;
	}
    
    public int getKpiset()
	{
		return kpiset;
	}
    
    public int getProcedureType()
	{
		return ProcedureType;
	}
    
	@Override
	public boolean equals(Object o)
	{//ProcedureType不作为key的一部分 tjz
		if (this == o){
			return true;
		}
			
		if (o == null || getClass() != o.getClass()){
			return false;
		}
			

		EventDataStatKey item = (EventDataStatKey) o;

		if (Interface == item.getInterface() 
				&& kpiset == item.getKpiset() 
				//&& ProcedureType == item.getProcedureType()
		   ){
			return true;
		}
			

		return false;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	@Override
	public String toString()
	{
		return Interface + "," + kpiset;// + "," + ProcedureType;
	}
}
