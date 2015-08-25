package proj.zoie.mbean;

import java.io.IOException;
import java.util.Date;

import proj.zoie.api.ZoieException;
import proj.zoie.impl.indexing.ZoieSystem;


public class ZoieSystemAdmin implements ZoieSystemAdminMBean {
	private final ZoieSystemAdminMBean _internalMBean;
	
	@SuppressWarnings("unchecked")
	public ZoieSystemAdmin(ZoieSystem zoieSystem)
	{
		_internalMBean=zoieSystem.getAdminMBean();
	}
	
	public void refreshDiskReader()  throws IOException{
		_internalMBean.refreshDiskReader();
	}

	public long getBatchDelay() {
		return _internalMBean.getBatchDelay();
	}

	public int getBatchSize() {
		return _internalMBean.getBatchSize();
	}

	public long getCurrentDiskVersion() throws IOException{
		return _internalMBean.getCurrentDiskVersion();
	}

	public int getDiskIndexSize() {
		return _internalMBean.getDiskIndexSize();
	}

	public String getDiskIndexerStatus() {
		return _internalMBean.getDiskIndexerStatus();
	}

	public Date getLastDiskIndexModifiedTime() {
		return _internalMBean.getLastDiskIndexModifiedTime();
	}

	public Date getLastOptimizationTime() {
		return _internalMBean.getLastOptimizationTime();
	}

	public int getMaxBatchSize() {
		return _internalMBean.getMaxBatchSize();
	}

	public long getOptimizationDuration() {
		return _internalMBean.getOptimizationDuration();
	}
	
	public int getRamAIndexSize() {
		return _internalMBean.getRamAIndexSize();
	}

	public long getRamAVersion() {
		return _internalMBean.getRamAVersion();
	}

	public int getRamBIndexSize() {
		return _internalMBean.getRamBIndexSize();
	}

	public long getRamBVersion() {
		return _internalMBean.getRamBVersion();
	}

	public void optimize(int numSegs) throws IOException {
		_internalMBean.optimize(numSegs);
	}

	public void setBatchSize(int batchSize) {
		_internalMBean.setBatchSize(batchSize);
	}

	public void setMaxBatchSize(int maxBatchSize) {
		_internalMBean.setMaxBatchSize(maxBatchSize);
	}

	public void setOptimizationDuration(long duration) {
		_internalMBean.setOptimizationDuration(duration);
	}

	public String getIndexDir() {
		return _internalMBean.getIndexDir();
	}

	public boolean isRealtime() {
		return _internalMBean.isRealtime();
	}

	public void setBatchDelay(long delay) {
		_internalMBean.setBatchDelay(delay);
	}

	public void flushToDiskIndex(long timeout) throws ZoieException{
		_internalMBean.flushToDiskIndex(timeout);
	}
	

	public void purgeIndex() throws IOException
	{
		_internalMBean.purgeIndex();
	}

	public void expungeDeletes() throws IOException {
		_internalMBean.expungeDeletes();
	}

	public int getMaxMergeDocs() {
		return _internalMBean.getMaxBatchSize();
	}

	public int getMergeFactor() {
		return _internalMBean.getMergeFactor();
	}

	public void setMaxMergeDocs(int maxMergeDocs) {
		_internalMBean.setMaxMergeDocs(maxMergeDocs);
	}

	public void setMergeFactor(int mergeFactor) {
		_internalMBean.setMergeFactor(mergeFactor);
	}

	public boolean isUseCompoundFile() {
		return _internalMBean.isUseCompoundFile();
	}

	public void setUseCompoundFile(boolean useCompoundFile) {
		_internalMBean.setUseCompoundFile(useCompoundFile);
	}

	public int getMaxSegments() {
		return _internalMBean.getMaxSegments();
	}

	public void setMaxSegments(int maxSegments) {
		_internalMBean.setMaxSegments(maxSegments);
	}
	
}
