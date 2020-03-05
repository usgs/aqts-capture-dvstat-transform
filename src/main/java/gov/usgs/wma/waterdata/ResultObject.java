package gov.usgs.wma.waterdata;

public class ResultObject {
	private String transformStatus;
	private Integer affectedTimeSteps;
	private Integer deletedTimeSteps;
	private Integer totalTimeSteps;
	public String getTransformStatus() {
		return transformStatus;
	}
	public void setTransformStatus(String transformStatus) {
		this.transformStatus = transformStatus;
	}
	public Integer getAffectedTimeSteps() {
		return affectedTimeSteps;
	}
	public void setAffectedTimeSteps(Integer affectedTimeSteps) {
		this.affectedTimeSteps = affectedTimeSteps;
	}
	public Integer getDeletedTimeSteps() {
		return deletedTimeSteps;
	}
	public void setDeletedTimeSteps(Integer deletedTimeSteps) {
		this.deletedTimeSteps = deletedTimeSteps;
	}
	public Integer getTotalTimeSteps() {
		return totalTimeSteps;
	}
	public void setTotalTimeSteps(Integer totalTimeSteps) {
		this.totalTimeSteps = totalTimeSteps;
	}
}
