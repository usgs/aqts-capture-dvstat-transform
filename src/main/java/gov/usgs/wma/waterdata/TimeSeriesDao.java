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

	@Value("classpath:sql/deleteTsCorrectedData.sql")
	private Resource deleteTsCorrectedData;

	@Value("classpath:sql/getExpectedPoints.sql")
	private Resource getExpectedPoints;

	@Value("classpath:sql/getGwStatisticalDvCount.sql")
	private Resource getGwStatisticalDvCount;

	@Value("classpath:sql/insertTsCorrectedData.sql")
	private Resource insertTsCorrectedData;

	@Value("classpath:sql/updateTsDescriptionList.sql")
	private Resource updateTsDescriptionList;

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
				request.getId(),
				request.getPartitionNumber(),
				request.getPartitionNumber());
	}

	@Transactional
	public int doUpdateTsDescriptionList(String uniqueId) {
		return jdbcTemplate.update(getSql(updateTsDescriptionList), uniqueId);
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
