package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
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
	classes={DBTestConfig.class, TimeSeriesDao.class, TransformDailyValue.class})
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
public class TransformDailyValueIT {

	@Autowired
	private TransformDailyValue transformDailyValue;
	private RequestObject request;

	public static final Integer TS_CORRECTED_ROWS_AFFECTED_1 = 1;
	public static final Integer TS_CORRECTED_ROWS_AFFECTED_2 = 12;

	public static final Integer TS_CORRECTED_ROWS_DELETED_2 = 9;

	public static final Integer TS_CORRECTED_ROWS_ABC = 22;

	public static final Long JSON_ID_0 = 0L;

	@BeforeEach
	public void beforeEach() {
		request = new RequestObject();
		request.setPartitionNumber(TimeSeriesDaoIT.PARTITION_NUMBER);
	}

	@DatabaseSetup("classpath:/testData/groundwaterStatisticalDailyValue/")
	@DatabaseSetup("classpath:/testData/insert/append/")
	@ExpectedDatabase(
			value="classpath:/testResult/insert/upsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@Test
	public void processTsCorrectedDataTest() {
		request.setId(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_2);
		request.setType(TransformDailyValue.TS_CORRECTED_DATA);
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		ResultObject result = transformDailyValue.processTsCorrectedData(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.TRANSFORM_ERROR, result.getTransformStatus());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_AFFECTED_2, result.getAffectedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_DELETED_2, result.getDeletedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());

		assertThrows(RuntimeException.class, () -> {
			transformDailyValue.apply(request);
		}, "should have thrown an exception but did not");
	}

	@DatabaseSetup("classpath:/testData/groundwaterStatisticalDailyValue/")
	@DatabaseSetup("classpath:/testData/update/")
	@ExpectedDatabase(
			value="classpath:/testResult/update/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	@Test
	public void processTsDescriptionListUpdate() {
		request.setId(JSON_ID_0);
		request.setType(TransformDailyValue.TS_DESCRIPTION_LIST);
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_DEF);
		ResultObject result = transformDailyValue.processTsDescriptionList(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		assertEquals(1, result.getAffectedTimeSteps());
		assertEquals(0, result.getDeletedTimeSteps());
		assertEquals(1, result.getTotalTimeSteps());
	}

	@Test
	public void processTsDescriptionListInsert() {
		request.setId(JSON_ID_0);
		request.setType(TransformDailyValue.TS_DESCRIPTION_LIST);
		request.setUniqueId("bad");
		ResultObject result = transformDailyValue.processTsDescriptionList(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.NOT_YET_IMPLEMENTED, result.getTransformStatus());
		assertNull(result.getAffectedTimeSteps());
		assertNull(result.getDeletedTimeSteps());
		assertNull(result.getTotalTimeSteps());

		assertThrows(RuntimeException.class, () -> {
			transformDailyValue.apply(request);
		}, "should have thrown an exception but did not");
	}

	@DatabaseSetup("classpath:/testData/groundwaterStatisticalDailyValue/")
	@DatabaseSetup("classpath:/testData/insert/append/")
	@Test
	public void validateTsCorrectedData() {
		request.setId(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_2);
		request.setType(TransformDailyValue.TS_CORRECTED_DATA);
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		ResultObject result = transformDailyValue.validateTsCorrectedData(request, 0, 9, 12);
		assertNotNull(result);
		assertEquals(TransformDailyValue.TRANSFORM_ERROR, result.getTransformStatus());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_AFFECTED_2, result.getAffectedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_DELETED_2, result.getDeletedTimeSteps());
		assertEquals(19, result.getTotalTimeSteps());

		assertThrows(RuntimeException.class, () -> {
			transformDailyValue.apply(request);
		}, "should have thrown an exception but did not");
	}

	@DatabaseSetup("classpath:/testData/groundwaterStatisticalDailyValue/")
	@DatabaseSetup("classpath:/testData/update/")
	@Test
	public void validateTsDescriptionList() {
		request.setId(JSON_ID_0);
		request.setType(TransformDailyValue.TS_DESCRIPTION_LIST);
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_DEF);
		ResultObject result = transformDailyValue.validateTsDescriptionList(request, 1, 1);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		assertEquals(1, result.getAffectedTimeSteps());
		assertEquals(0, result.getDeletedTimeSteps());
		assertEquals(1, result.getTotalTimeSteps());
	}
}
