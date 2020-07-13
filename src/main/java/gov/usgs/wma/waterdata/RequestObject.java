package gov.usgs.wma.waterdata;

public class RequestObject {
	private Long id;
	private String uniqueId;
	private String type;
	private Integer partitionNumber;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getUniqueId() {
		return uniqueId;
	}
	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}

	public Integer getPartitionNumber() {
		return partitionNumber;
	}

	public void setPartitionNumber(Integer partitionNumber) {
		this.partitionNumber = partitionNumber;
	}
}
