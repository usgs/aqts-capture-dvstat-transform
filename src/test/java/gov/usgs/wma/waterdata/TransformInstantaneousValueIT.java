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
        classes={DBTestConfig.class, TimeSeriesDao.class, TransformInstantaneousValue.class})
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
public class TransformInstantaneousValueIT {

    @Autowired
    private TransformInstantaneousValue transformInstantaneousValue;
    private RequestObject request;

    public static final Integer INSTANTANEOUSTS_ROWS_AFFECTED_1 = 1;
    public static final Integer INSTANTANEOUSTS_ROWS_AFFECTED_2 = 12;

    public static final Integer INSTANTANEOUSTS_ROWS_DELETED_2 = 9;

    public static final Integer INSTANTANEOUS_ROWS_d53f1e5a50aa49adb04dc52ad04c4701 = 15;

    public static final Long JSON_ID_0 = 0L;

    @BeforeEach
    public void beforeEach() {
        request = new RequestObject();
        request.setPartitionNumber(TimeSeriesDaoIT.INSTANTANEOUS_PARTITION_NUMBER);
    }

    @DatabaseSetup("classpath:/testData/instantaneous/instantaneousValue/")
    @DatabaseSetup("classpath:/testData/instantaneous/insert/append/")
    @ExpectedDatabase(
            value="classpath:/testResult/instantaneous/insert/upsert/",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
    )
    @Test
    public void processInstantaneousDataTest() {
        request.setId(TimeSeriesDaoIT.INSTANTANEOUS_JSON_DATA_ID_106974156);
        request.setType(TransformInstantaneousValue.TS_CORRECTED_DATA);
        request.setUniqueId(TimeSeriesDaoIT.INSTANTANEOUS_TIME_SERIES_UNIQUE_ID);
        ResultObject result = transformInstantaneousValue.processInstantaneousData(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, result.getTransformStatus());
        assertEquals(TransformInstantaneousValueIT.INSTANTANEOUSTS_ROWS_AFFECTED_2, result.getAffectedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.INSTANTANEOUSTS_ROWS_DELETED_2, result.getDeletedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.INSTANTANEOUS_ROWS_d53f1e5a50aa49adb04dc52ad04c4701, result.getTotalTimeSteps());
    }

    @DatabaseSetup("classpath:/testData/instantaneous/instantaneousValue/")
    @DatabaseSetup("classpath:/testData/instantaneous/update/")
    @ExpectedDatabase(
            value="classpath:/testResult/instantaneous/update/",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
    )
    @Test
    public void processTsDescriptionListInstantaneousUpdate() {
        request.setId(JSON_ID_0);
        request.setType(TransformInstantaneousValue.TS_DESCRIPTION_LIST);
        request.setUniqueId(TimeSeriesDaoIT.INSTANTANEOUS_TIME_SERIES_UNIQUE_ID_2);
        ResultObject result = transformInstantaneousValue.processTsDescriptionListInstantaneous(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, result.getTransformStatus());
        assertEquals(1, result.getAffectedTimeSteps());
        assertEquals(0, result.getDeletedTimeSteps());
        assertEquals(1, result.getTotalTimeSteps());
    }

    @Test
    public void processTsDescriptionListInstantaneousInstantaneousInsert() {
        request.setId(JSON_ID_0);
        request.setType(TransformInstantaneousValue.TS_DESCRIPTION_LIST);
        request.setUniqueId("bad");
        ResultObject result = transformInstantaneousValue.processTsDescriptionListInstantaneous(request);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.NOT_YET_IMPLEMENTED, result.getTransformStatus());
        assertNull(result.getAffectedTimeSteps());
        assertNull(result.getDeletedTimeSteps());
        assertNull(result.getTotalTimeSteps());

        assertThrows(RuntimeException.class, () -> {
            transformInstantaneousValue.apply(request);
        }, "should have thrown an exception but did not");
    }

    @DatabaseSetup("classpath:/testData/instantaneous/instantaneousValue/")
    @DatabaseSetup("classpath:/testData/instantaneous/insert/append/")
    @Test
    public void validateInstantaneousData() {
        request.setId(TimeSeriesDaoIT.INSTANTANEOUS_JSON_DATA_ID_106974156);
        request.setType(TransformInstantaneousValue.TS_CORRECTED_DATA);
        request.setUniqueId(TimeSeriesDaoIT.INSTANTANEOUS_TIME_SERIES_UNIQUE_ID);
        ResultObject result = transformInstantaneousValue.validateInstantaneousData(request, 0, 9, 12);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.TRANSFORM_ERROR, result.getTransformStatus());
        assertEquals(TransformInstantaneousValueIT.INSTANTANEOUSTS_ROWS_AFFECTED_2, result.getAffectedTimeSteps());
        assertEquals(TransformInstantaneousValueIT.INSTANTANEOUSTS_ROWS_DELETED_2, result.getDeletedTimeSteps());
        assertEquals(12, result.getTotalTimeSteps());
    }

    @DatabaseSetup("classpath:/testData/instantaneous/instantaneousValue/")
    @DatabaseSetup("classpath:/testData/instantaneous/update/")
    @Test
    public void validateTsDescriptionListInstantaneous() {
        request.setId(JSON_ID_0);
        request.setType(TransformInstantaneousValue.TS_DESCRIPTION_LIST);
        request.setUniqueId(TimeSeriesDaoIT.INSTANTANEOUS_TIME_SERIES_UNIQUE_ID_2);
        ResultObject result = transformInstantaneousValue.validateTsDescriptionListInstantaneous(request, 1, 1);
        assertNotNull(result);
        assertEquals(TransformInstantaneousValue.SUCCESSFUL, result.getTransformStatus());
        assertEquals(1, result.getAffectedTimeSteps());
        assertEquals(0, result.getDeletedTimeSteps());
        assertEquals(1, result.getTotalTimeSteps());
    }
}
