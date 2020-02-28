package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest(webEnvironment=WebEnvironment.NONE)
public class TransformDailyValueTest {

	private TransformDailyValue transformDailyValue;
	@MockBean
	private TimeSeriesDao timeSeriesDao;

	@BeforeEach
	public void beforeEach() {
		transformDailyValue = new TransformDailyValue(timeSeriesDao);
		when(timeSeriesDao.doInsertTsCorrectedData(TransformDailyValueIT.TS_CORRECTED_JSON_DATA_ID_1))
			.thenReturn(TransformDailyValueIT.TS_CORRECTED_ROWS_AFFECTED_1);
//		when(timeSeriesDao.doCountGwStatisticalDailyValue(TransformDailyValueIT.TS_CORRECTED_ID_ABC))
//			.thenReturn(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC);
	}

	@Test
	public void tsCorrectedDataTest() {
		RequestObject request = new RequestObject();
		request.setId(TransformDailyValueIT.TS_CORRECTED_JSON_DATA_ID_1);
		request.setType(TransformDailyValue.TS_CORRECTED_DATA);
		request.setUniqueId(TransformDailyValueIT.TS_CORRECTED_ID_ABC);
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_AFFECTED_1, result.getAffectedTimeSteps());
		assertEquals(TransformDailyValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
	}

	@Test
	public void tsDescriptionListTest() {
		RequestObject request = new RequestObject();
		request.setType(TransformDailyValue.TS_DESCRIPTION_LIST);
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
		assertEquals(3, result.getAffectedTimeSteps());
		assertEquals(4, result.getTotalTimeSteps());
	}

	@Test
	public void badInputTest() {
		RequestObject request = new RequestObject();
		request.setType("abc");
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.BAD_INPUT, result.getTransformStatus());
		assertNull(result.getAffectedTimeSteps());
		assertNull(result.getTotalTimeSteps());
	}

	@Test
	public void processRequestInvalidRequestNullRequestTest() {
		ResultObject result = transformDailyValue.processRequest(null);
		assertNotNull(result);
		assertEquals(TransformDailyValue.BAD_INPUT, result.getTransformStatus());
	}

	@Test
	public void processRequestInvalidRequestNullRequestTypeTest() {
		RequestObject request = new RequestObject();
		request.setType(null);
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.BAD_INPUT, result.getTransformStatus());
	}

	@Test
	public void processRequestInvalidRequestInvalidRequestTypeTest() {
		RequestObject request = new RequestObject();
		request.setType("abc");
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.BAD_INPUT, result.getTransformStatus());
	}

	@Test
	public void processRequestTsCorrectedDataTest() {
		RequestObject request = new RequestObject();
		request.setType(TransformDailyValue.TS_CORRECTED_DATA);
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
	}

	@Test
	public void processRequestTsDescriptionListTest() {
		RequestObject request = new RequestObject();
		request.setType(TransformDailyValue.TS_DESCRIPTION_LIST);
		ResultObject result = transformDailyValue.processRequest(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.SUCCESSFUL, result.getTransformStatus());
	}

	@Test
	public void applyTest() {
		RequestObject request = new RequestObject();
		request.setType("abc");
		ResultObject result = transformDailyValue.apply(request);
		assertNotNull(result);
		assertEquals(TransformDailyValue.BAD_INPUT, result.getTransformStatus());
	}
}
