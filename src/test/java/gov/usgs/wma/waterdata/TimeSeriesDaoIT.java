package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.FileUrlResource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

@SpringBootTest(webEnvironment=WebEnvironment.NONE,
	classes={DBTestConfig.class, TimeSeriesDao.class})
@ActiveProfiles("it")
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class,
	DirtiesContextTestExecutionListener.class,
	TransactionalTestExecutionListener.class,
	TransactionDbUnitTestExecutionListener.class })
@DbUnitConfiguration(dataSetLoader=FileSensingDataSetLoader.class)
@AutoConfigureTestDatabase(replace=Replace.NONE)
@Transactional(propagation=Propagation.NOT_SUPPORTED)
@Import({DBTestConfig.class})
@DirtiesContext
public class TimeSeriesDaoIT {

	@Autowired
	private TimeSeriesDao timeSeriesDao;

	public static final Long TS_CORRECTED_JSON_DATA_ID_1 = 1l;
	public static final Long TS_CORRECTED_JSON_DATA_ID_2 = 2l;
	public static final Long TS_CORRECTED_JSON_DATA_ID_3 = 3l;

	public static final String TS_CORRECTED_ID_ABC = "aBc";
	public static final String TS_CORRECTED_ID_DEF = "dEf";

	@DatabaseSetup("classpath:/testData/groundwaterStatisticalDailyValue/")
	@DatabaseSetup("classpath:/testData/delete/entire/")
	@ExpectedDatabase(
			value="classpath:/testResult/delete/entire/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@Test
	public void doDeleteTsCorrectedDataEntireTest() {
		assertEquals(19, timeSeriesDao.doDeleteTsCorrectedData(TS_CORRECTED_JSON_DATA_ID_1));
	}

	@DatabaseSetup("classpath:/testData/groundwaterStatisticalDailyValue/")
	@DatabaseSetup("classpath:/testData/delete/partial/")
	@ExpectedDatabase(
			value="classpath:/testResult/delete/partial/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@Test
	public void doDeleteTsCorrectedDataPartialTest() {
		assertEquals(6, timeSeriesDao.doDeleteTsCorrectedData(TS_CORRECTED_JSON_DATA_ID_1));
	}

	@DatabaseSetup("classpath:/testData/delete/entire/")
	@Test
	public void doGetExpectedPointsTest() {
		assertEquals(19, timeSeriesDao.doGetExpectedPoints(TS_CORRECTED_JSON_DATA_ID_1));
	}

	@DatabaseSetup("classpath:/testData/groundwaterStatisticalDailyValue/")
	@Test
	public void doGetGwStatisticalDvCountTest() {
		assertEquals(19, timeSeriesDao.doGetGwStatisticalDvCount(TS_CORRECTED_ID_ABC));
		assertEquals(1, timeSeriesDao.doGetGwStatisticalDvCount(TS_CORRECTED_ID_DEF));
	}

	@DatabaseSetup("classpath:/testData/groundwaterStatisticalDailyValue/")
	@DatabaseSetup("classpath:/testData/insert/append/")
//	@ExpectedDatabase(
//			value="classpath:/testResult/insert/append/",
//			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
//			)
	@Test
	public void doInsertTsCorrectedDataTest() {
		assertEquals(12, timeSeriesDao.doInsertTsCorrectedData(TS_CORRECTED_JSON_DATA_ID_2));
		assertEquals(13, timeSeriesDao.doInsertTsCorrectedData(TS_CORRECTED_JSON_DATA_ID_3));
	}

	@Test
	public void badResource() {
		try {
			timeSeriesDao.getSql(new FileUrlResource("classpath:sql/missing.sql"));
			fail("Should have gotten a RuntimeException");
		} catch (Exception e) {
			assertTrue(e instanceof RuntimeException);
			assertEquals("java.io.FileNotFoundException: classpath:sql/missing.sql (No such file or directory)",
					e.getMessage());
		}
	}
}
