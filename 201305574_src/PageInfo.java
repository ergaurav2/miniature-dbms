
public class PageInfo {

	private int startrecordid = 0;
	
	private int endrecordid = 0;
	
	private int pagesize;
	
	private int pageno;
	
	private int beiginpoint;
	
	private int endpoint;
	

	public int getBeiginpoint() {
		return beiginpoint;
	}

	public void setBeiginpoint(int beiginpoint) {
		this.beiginpoint = beiginpoint;
	}

	public int getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(int endpoint) {
		this.endpoint = endpoint;
	}

	private boolean inmemory = false;

	public boolean isInmemory() {
		return inmemory;
	}

	public void setInmemory(boolean inmemory) {
		this.inmemory = inmemory;
	}

	public int getStartrecordid() {
		return startrecordid;
	}

	public void setStartrecordid(int startrecordid) {
		this.startrecordid = startrecordid;
	}

	public int getEndrecordid() {
		return endrecordid;
	}

	public void setEndrecordid(int endrecordid) {
		this.endrecordid = endrecordid;
	}

	public int getPagesize() {
		return pagesize;
	}

	public void setPagesize(int pagesize) {
		this.pagesize = pagesize;
	}

	public int getPageno() {
		return pageno;
	}

	public void setPageno(int pageno) {
		this.pageno = pageno;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Pageno: "+pageno);
		sb.append(" Start: "+startrecordid);
		sb.append("End: "+endrecordid);
		sb.append(" Pagesize: "+pagesize);
		sb.append("In memory: "+isInmemory());
		sb.append("Begin point: "+getBeiginpoint());
		sb.append("End point: "+getEndpoint());
		
		return sb.toString();
	}
}
