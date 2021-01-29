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
public class TransformInstantaneousValueTest {

    private TransformInstantaneousValue transformInstantaneousValue;
    private RequestObject request;
    @MockBean
    private TimeSeriesDao timeSeriesDao;

    @BeforeEach
    public void beforeEach() {
        transformInstantaneousValue = new TransformInstantaneousValue(timeSeriesDao);
        request = new RequestObject();

        when(timeSeriesDao.doInsertInstantaneousData(request))
                .thenReturn(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_AFFECTED_1);
        when(timeSeriesDao.doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC))
                .thenReturn(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC);
        when(timeSeriesDao.doUpdateTsDescriptionListInstantaneous(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC))
                .thenReturn(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC);
    }

    @Test
    public void applyTest() {
        request.setType("abc");
        assertThrows(RuntimeException.class, () -> {
            transformInstantaneousValue.apply(request);
        }, "should have thrown an exception but did not");
    }

    @Test
    public void processRequestInvalidRequestNullRequestTest() {
        ResultObject result = transformInstantaneousValue.processRequest(null);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.BAD_INPUT, result.getTransformStatus());
        assertNull(result.getAffectedTimeSteps());
        assertNull(result.getTotalTimeSteps());

        assertThrows(RuntimeException.class, () -> {
            transformInstantaneousValue.apply(request);
        }, "should have thrown an exception but did not");
    }

    @Test
    public void processRequestInvalidRequestNullRequestTypeTest() {
        request.setType(null);
        ResultObject result = transformInstantaneousValue.processRequest(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.BAD_INPUT, result.getTransformStatus());
        assertNull(result.getAffectedTimeSteps());
        assertNull(result.getTotalTimeSteps());

        assertThrows(RuntimeException.class, () -> {
            transformInstantaneousValue.apply(request);
        }, "should have thrown an exception but did not");
    }

    @Test
    public void processRequestInvalidRequestInvalidRequestTypeTest() {
        request.setType("abc");
        ResultObject result = transformInstantaneousValue.processRequest(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.BAD_INPUT, result.getTransformStatus());
        assertNull(result.getAffectedTimeSteps());
        assertNull(result.getTotalTimeSteps());

        assertThrows(RuntimeException.class, () -> {
            transformInstantaneousValue.apply(request);
        }, "should have thrown an exception but did not");
    }

    @Test
    public void processRequestTsCorrectedDataTest() {
        request.setId(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
        request.setType(TransformInstantaneousValue.TS_CORRECTED_DATA);
        request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
        ResultObject result = transformInstantaneousValue.processRequest(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.TRANSFORM_ERROR, result.getTransformStatus());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_AFFECTED_1, result.getAffectedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
        verify(timeSeriesDao, times(2)).doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
        verify(timeSeriesDao).doDeleteInstantaneousData(request);
        verify(timeSeriesDao).doInsertInstantaneousData(request);
        verify(timeSeriesDao).doGetInstantaneousExpectedPoints(request);
    }

    @Test
    public void processRequestTsDescriptionListTest() {
        request.setType(TransformInstantaneousValue.TS_DESCRIPTION_LIST);
        request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
        ResultObject result = transformInstantaneousValue.processRequest(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, result.getTransformStatus());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getAffectedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
        verify(timeSeriesDao, times(2)).doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
        verify(timeSeriesDao).doUpdateTsDescriptionListInstantaneous(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
    }

    @Test
    public void processInstantaneousDataTest() {
        RequestObject requestObject = new RequestObject();
        requestObject.setType(TransformInstantaneousValue.TS_CORRECTED_DATA);
        ResultObject result = transformInstantaneousValue.processInstantaneousData(requestObject);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, result.getTransformStatus());
        verify(timeSeriesDao, times(2)).doGetInstantaneousCount(null);
        verify(timeSeriesDao).doDeleteInstantaneousData(requestObject);
        verify(timeSeriesDao).doInsertInstantaneousData(requestObject);
        verify(timeSeriesDao).doGetInstantaneousExpectedPoints(requestObject);
    }

    @Test
    public void processTsDescriptionListInstantaneousUpdateTest() {
        request.setType(TransformInstantaneousValue.TS_DESCRIPTION_LIST);
        request.setId(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
        request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
        ResultObject result = transformInstantaneousValue.processTsDescriptionListInstantaneous(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, result.getTransformStatus());
        verify(timeSeriesDao, times(2)).doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
        verify(timeSeriesDao).doUpdateTsDescriptionListInstantaneous(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
    }

    @Test
    public void processTsDescriptionListInstantaneousInstantaneousInsertTest() {
        request.setType(TransformInstantaneousValue.TS_DESCRIPTION_LIST);
        ResultObject result = transformInstantaneousValue.processTsDescriptionListInstantaneous(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.NOT_YET_IMPLEMENTED, result.getTransformStatus());
        assertNull(result.getAffectedTimeSteps());
        assertNull(result.getDeletedTimeSteps());
        assertNull(result.getTotalTimeSteps());
        verify(timeSeriesDao).doGetInstantaneousCount(null);
    }

    @Test
    public void findAndprocessInstantaneousDataTest() {
        ResultObject result = transformInstantaneousValue.findAndprocessInstantaneousData(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.NOT_YET_IMPLEMENTED, result.getTransformStatus());
        assertNull(result.getAffectedTimeSteps());
        assertNull(result.getDeletedTimeSteps());
        assertNull(result.getTotalTimeSteps());
    }

    @Test
    public void validateInstantaneousDataTest() {
        request.setId(TimeSeriesDaoIT.TS_CORRECTED_JSON_DATA_ID_1);
        request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
        ResultObject result = transformInstantaneousValue.validateInstantaneousData(request, 1, 2, 3);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.TRANSFORM_ERROR, result.getTransformStatus());
        assertEquals(3, result.getAffectedTimeSteps());
        assertEquals(2, result.getDeletedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
        verify(timeSeriesDao).doGetInstantaneousExpectedPoints(request);
        verify(timeSeriesDao).doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);

        result = transformInstantaneousValue.validateInstantaneousData(request, TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, 0, 0);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, result.getTransformStatus());
        assertEquals(0, result.getAffectedTimeSteps());
        assertEquals(0, result.getDeletedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
        verify(timeSeriesDao, times(2)).doGetInstantaneousExpectedPoints(request);
        verify(timeSeriesDao, times(2)).doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
    }

    @Test
    public void validateTsDescriptionListInstantaneousTest() {
        request.setUniqueId(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
        ResultObject result = transformInstantaneousValue.validateTsDescriptionListInstantaneous(request, 25, 7);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.TRANSFORM_ERROR, result.getTransformStatus());
        assertEquals(7, result.getAffectedTimeSteps());
        assertEquals(0, result.getDeletedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
        verify(timeSeriesDao).doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);

        result = transformInstantaneousValue.validateTsDescriptionListInstantaneous(request, 25, 25);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.TRANSFORM_ERROR, result.getTransformStatus());
        assertEquals(25, result.getAffectedTimeSteps());
        assertEquals(0, result.getDeletedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
        verify(timeSeriesDao, times(2)).doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);

        result = transformInstantaneousValue.validateTsDescriptionListInstantaneous(request, TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, result.getTransformStatus());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getAffectedTimeSteps());
        assertEquals(0, result.getDeletedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.TS_CORRECTED_ROWS_ABC, result.getTotalTimeSteps());
        verify(timeSeriesDao, times(3)).doGetInstantaneousCount(TimeSeriesDaoIT.TS_CORRECTED_ID_ABC);
    }

    @Test
    public void determineStatusTest() {
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, transformInstantaneousValue.determineStatus(5, 4, 3, 4, 3));
        assertEquals(TransformInstantaneousValue.TRANSFORM_ERROR, transformInstantaneousValue.determineStatus(5, 4, 3, 4, 1));
        assertEquals(TransformInstantaneousValue.TRANSFORM_ERROR, transformInstantaneousValue.determineStatus(5, 4, 3, 2, 1));
    }
}
