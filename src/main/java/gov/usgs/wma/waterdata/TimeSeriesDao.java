package gov.usgs.wma.waterdata;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.FileCopyUtils;

@Component
public class TimeSeriesDao {
	private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesDao.class);

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	// Statistical daily value queries
	@Value("classpath:sql/statisticalDailyValue/deleteTsCorrectedData.sql")
	private Resource deleteTsCorrectedData;

	@Value("classpath:sql/statisticalDailyValue/getExpectedPoints.sql")
	private Resource getExpectedPoints;

	@Value("classpath:sql/statisticalDailyValue/getGwStatisticalDvCount.sql")
	private Resource getGwStatisticalDvCount;

	@Value("classpath:sql/statisticalDailyValue/insertTsCorrectedData.sql")
	private Resource insertTsCorrectedData;

	@Value("classpath:sql/statisticalDailyValue/updateTsDescriptionList.sql")
	private Resource updateTsDescriptionList;

	// Instantaneous value queries
	@Value("classpath:sql/instantaneous/deleteInstantaneousData.sql")
	private Resource deleteInstantaneousData;

	@Value("classpath:sql/instantaneous/getInstantaneousExpectedPoints.sql")
	private Resource getInstantaneousExpectedPoints;

	@Value("classpath:sql/instantaneous/getInstantaneousCount.sql")
	private Resource getInstantaneousCount;

	@Value("classpath:sql/instantaneous/insertInstantaneousData.sql")
	private Resource insertInstantaneousData;

	@Value("classpath:sql/instantaneous/updateTsDescriptionListInstantaneous.sql")
	private Resource updateTsDescriptionListInstantaneous;

	@Transactional
	public int doDeleteTsCorrectedData(RequestObject request) {
		return jdbcTemplate.update(
				getSql(deleteTsCorrectedData),
				request.getId(),
				request.getPartitionNumber());
	}

	@Transactional(readOnly=true)
	public Integer doGetExpectedPoints(RequestObject request) {
		return jdbcTemplate.queryForObject(
				getSql(getExpectedPoints),
				Integer.class,
				request.getId(),
				request.getPartitionNumber());
	}

	@Transactional(readOnly=true)
	public Integer doGetGwStatisticalDvCount(String uniqueId) {
		return jdbcTemplate.queryForObject(getSql(getGwStatisticalDvCount), Integer.class, uniqueId);
	}

	@Transactional
	public int doInsertTsCorrectedData(RequestObject request) {
		return jdbcTemplate.update(
				getSql(insertTsCorrectedData),
				request.getPartitionNumber(),
				request.getPartitionNumber(),
				request.getPartitionNumber(),
				request.getId(),
				request.getPartitionNumber(),
				request.getPartitionNumber());
	}

	@Transactional
	public int doUpdateTsDescriptionList(String uniqueId) {
		return jdbcTemplate.update(getSql(updateTsDescriptionList), uniqueId);
	}

	@Transactional
	public int doDeleteInstantaneousData(RequestObject request) {
		return jdbcTemplate.update(
				getSql(deleteInstantaneousData),
				request.getId(),
				request.getPartitionNumber());
	}

	@Transactional(readOnly=true)
	public Integer doGetInstantaneousExpectedPoints(RequestObject request) {
		return jdbcTemplate.queryForObject(
				getSql(getInstantaneousExpectedPoints),
				Integer.class,
				request.getId(),
				request.getPartitionNumber());
	}

	@Transactional(readOnly=true)
	public Integer doGetInstantaneousCount(String uniqueId) {
		return jdbcTemplate.queryForObject(getSql(getInstantaneousCount), Integer.class, uniqueId);
	}

	@Transactional
	public int doInsertInstantaneousData(RequestObject request) {
		return jdbcTemplate.update(
				getSql(insertInstantaneousData),
				request.getPartitionNumber(),
				request.getPartitionNumber(),
				request.getPartitionNumber(),
				request.getId(),
				request.getPartitionNumber(),
				request.getPartitionNumber());
	}

	@Transactional
	public int doUpdateTsDescriptionListInstantaneous(String uniqueId) {
		return jdbcTemplate.update(getSql(updateTsDescriptionListInstantaneous), uniqueId);
	}

	protected String getSql(Resource resource) {
		String sql = null;
		try {
			sql = new String(FileCopyUtils.copyToByteArray(resource.getInputStream()));
		} catch (IOException e) {
			LOG.error("Unable to get SQL statement", e);
			throw new RuntimeException(e);
		}
		return sql;
	}
}
