package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest(webEnvironment=WebEnvironment.NONE)
public class TransformDailyValueTest {

	private TransformDailyValue transformDailyValue;
	private RequestObject request;
	@MockBean
	private TimeSeriesDao timeSeriesDao;

	@BeforeEach
	public void beforeEach() {
		transformDailyValue = new TransformDailyValue(timeSeriesDao);
		request = new RequestObject();
		when(timeSeriesDao.doInsertTsCorrectedData(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1))
			.thenReturn(TransformDailyValueIT.TS_CORRECTED_ROWS_AFFECTED_1);
		when(timeSeriesDao.doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC))
			.thenReturn(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC);
		when(timeSeriesDao.doUpdateTsDescriptionList(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC))
			.thenReturn(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC);
	}

	@Test
	public void applyTest() {
		request.setType("abc");
		assertThrows(RuntimeException.class, () -> {
			transformDailyValue.apply(request);
		}, "should have thrown an exception but did not");
	}

	@Test
	public void processRequestInvalidRequestNullRequestTest() {
		ResultObject result = transformDailyValue.processRequest(null);
		assertNotNull(result);
		assertEquals(TransformDailyValue.BAD_INPUT, result.getTransformStatus());
		assertNull(result.getAffectedTimeSteps());
		assertNull(result.getTotalTimeSteps());

		assertThrows(RuntimeException.class, () -> {
			transformDailyValue.apply(request);
		}, "should have thrown an exception but did not");
	}

	@Test
	public void processRequestInvalidRequestNullRequestTypeTest() {
		request.setType(null);
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.BAD_INPUT, result.getTransformStatus());
		assertNull(result.getAffectedTimeSteps());
		assertNull(result.getTotalTimeSteps());

		assertThrows(RuntimeException.class, () -> {
			transformDailyValue.apply(request);
		}, "should have thrown an exception but did not");
	}

	@Test
	public void processRequestInvalidRequestInvalidRequestTypeTest() {
		request.setType("abc");
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.BAD_INPUT, result.getTransformStatus());
		assertNull(result.getAffectedTimeSteps());
		assertNull(result.getTotalTimeSteps());

		assertThrows(RuntimeException.class, () -> {
			transformDailyValue.apply(request);
		}, "should have thrown an exception but did not");
	}

	@Test
	public void processRequestTsCorrectedDataTest() {
		request.setId(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
		request.setType(TransformDailyValue.TS_CORRECTED_DATA);
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.TRANSFORM_ERROR, result.getTransformStatus());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_AFFECTED_1, result.getAffectedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
		verify(timeSeriesDao, times(2)).doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		verify(timeSeriesDao).doDeleteTsCorrectedData(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
		verify(timeSeriesDao).doInsertTsCorrectedData(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
		verify(timeSeriesDao).doGetExpectedPoints(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
	}

	@Test
	public void processRequestTsDescriptionListTest() {
		request.setType(TransformDailyValue.TS_DESCRIPTION_LIST);
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getAffectedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
		verify(timeSeriesDao, times(2)).doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		verify(timeSeriesDao).doUpdateTsDescriptionList(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
	}

	@Test
	public void processTsCorrectedDataTest() {
		request.setType(TransformDailyValue.TS_CORRECTED_DATA);
		ResultObject result = transformDailyValue.processTsCorrectedData(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		verify(timeSeriesDao, times(2)).doGetGwStatisticalDvCount(null);
		verify(timeSeriesDao).doDeleteTsCorrectedData(null);
		verify(timeSeriesDao).doInsertTsCorrectedData(null);
		verify(timeSeriesDao).doGetExpectedPoints(null);
	}

	@Test
	public void processTsDescriptionListUpdateTest() {
		request.setType(TransformDailyValue.TS_DESCRIPTION_LIST);
		request.setId(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		ResultObject result = transformDailyValue.processTsDescriptionList(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		verify(timeSeriesDao, times(2)).doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		verify(timeSeriesDao).doUpdateTsDescriptionList(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
	}

	@Test
	public void processTsDescriptionListInsertTest() {
		request.setType(TransformDailyValue.TS_DESCRIPTION_LIST);
		ResultObject result = transformDailyValue.processTsDescriptionList(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.NOT_YET_IMPLEMENTED, result.getTransformStatus());
		assertNull(result.getAffectedTimeSteps());
		assertNull(result.getDeletedTimeSteps());
		assertNull(result.getTotalTimeSteps());
		verify(timeSeriesDao).doGetGwStatisticalDvCount(null);
	}

	@Test
	public void findAndProcessTsCorrectedDataTest() {
		ResultObject result = transformDailyValue.findAndProcessTsCorrectedData(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.NOT_YET_IMPLEMENTED, result.getTransformStatus());
		assertNull(result.getAffectedTimeSteps());
		assertNull(result.getDeletedTimeSteps());
		assertNull(result.getTotalTimeSteps());
	}

	@Test
	public void validateTsCorrectedDataTest() {
		request.setId(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		ResultObject result = transformDailyValue.validateTsCorrectedData(request, 1, 2, 3);
		assertNotNull(result);
		assertEquals(TransformDailyValue.TRANSFORM_ERROR, result.getTransformStatus());
		assertEquals(3, result.getAffectedTimeSteps());
		assertEquals(2, result.getDeletedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
		verify(timeSeriesDao).doGetExpectedPoints(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
		verify(timeSeriesDao).doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);

		result = transformDailyValue.validateTsCorrectedData(request, TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, 0, 0);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		assertEquals(0, result.getAffectedTimeSteps());
		assertEquals(0, result.getDeletedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
		verify(timeSeriesDao, times(2)).doGetExpectedPoints(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
		verify(timeSeriesDao, times(2)).doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
	}

	@Test
	public void validateTsDescriptionListTest() {
		request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
		ResultObject result = transformDailyValue.validateTsDescriptionList(request, 25, 7);
		assertNotNull(result);
		assertEquals(TransformDailyValue.TRANSFORM_ERROR, result.getTransformStatus());
		assertEquals(7, result.getAffectedTimeSteps());
		assertEquals(0, result.getDeletedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
		verify(timeSeriesDao).doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);

		result = transformDailyValue.validateTsDescriptionList(request, 25, 25);
		assertNotNull(result);
		assertEquals(TransformDailyValue.TRANSFORM_ERROR, result.getTransformStatus());
		assertEquals(25, result.getAffectedTimeSteps());
		assertEquals(0, result.getDeletedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
		verify(timeSeriesDao, times(2)).doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);

		result = transformDailyValue.validateTsDescriptionList(request, TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, TransformDailyValueIT.TS_CORRECTED_ROWS_ABC);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getAffectedTimeSteps());
		assertEquals(0, result.getDeletedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
		verify(timeSeriesDao, times(3)).doGetGwStatisticalDvCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
	}

	@Test
	public void determineStatusTest() {
		assertEquals(TransformDailyValue.SUCCESSFUL, transformDailyValue.determineStatus(5, 4, 3, 4, 3));
		assertEquals(TransformDailyValue.TRANSFORM_ERROR, transformDailyValue.determineStatus(5, 4, 3, 4, 1));
		assertEquals(TransformDailyValue.TRANSFORM_ERROR, transformDailyValue.determineStatus(5, 4, 3, 2, 1));
	}
}
