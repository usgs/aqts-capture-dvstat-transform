package gov.usgs.wma.waterdata;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("instantaneous")
public class TransformInstantaneousValue implements Function<RequestObject, ResultObject> {
    private static final Logger LOG = LoggerFactory.getLogger(TransformInstantaneousValue.class);

    private TimeSeriesDao timeSeriesDao;

    public static final String TS_CORRECTED_DATA = "tsCorrectedData";
    public static final String TS_DESCRIPTION_LIST = "tsDescriptionList";

    public static final String BAD_INPUT = "badInput";
    public static final String TRANSFORM_ERROR = "transformError";
    public static final String NOT_YET_IMPLEMENTED = "notYetImplemented";
    public static final String SUCCESSFUL = "successful";

    @Autowired
    public TransformInstantaneousValue(TimeSeriesDao timeSeriesDao) {
        this.timeSeriesDao = timeSeriesDao;
    }

    @Override
    public ResultObject apply(RequestObject request) {
        ResultObject result = processRequest(request);
        String transformStatus = result.getTransformStatus();
        if (SUCCESSFUL != transformStatus) {
            throw new RuntimeException(transformStatus);
        } else {
            return result;
        }
    }

    protected ResultObject processRequest(RequestObject request) {
        if (null != request && null != request.getType()) {
            LOG.debug("requestType {} for id {} and timeseries {}", request.getType(), request.getId(), request.getUniqueId());
            switch (request.getType()) {
                case TS_CORRECTED_DATA:
                    return processInstantaneousData(request);
                case TS_DESCRIPTION_LIST:
                    return processTsDescriptionListInstantaneous(request);
                default:
                    return badInput(request);
            }
        } else {
            LOG.debug("request or type was null");
            return badInput(request);
        }
    }

    protected ResultObject processInstantaneousData(RequestObject request) {
        int initialCount = timeSeriesDao.doGetInstantaneousCount(request.getUniqueId());
        int deletedCount = timeSeriesDao.doDeleteInstantaneousData(request);
        int affectedCount = timeSeriesDao.doInsertInstantaneousData(request);
        return validateInstantaneousData(request, initialCount, deletedCount, affectedCount);
    }

    protected ResultObject processTsDescriptionListInstantaneous(RequestObject request) {
        ResultObject result = null;
        int initialCount = timeSeriesDao.doGetInstantaneousCount(request.getUniqueId());
        if (0 == initialCount) {
            //Assume the TsCorrectedData was processed first and failed, so process it again
            result = findAndprocessInstantaneousData(request);
        } else {
            //Just update existing rows
            int affectedCount = timeSeriesDao.doUpdateTsDescriptionListInstantaneous(request.getUniqueId());
            result = validateTsDescriptionListInstantaneous(request, initialCount, affectedCount);
        }
        return result;
    }

    protected ResultObject findAndprocessInstantaneousData(RequestObject request) {
        ResultObject result = new ResultObject();
        result.setTransformStatus(NOT_YET_IMPLEMENTED);
        return result;
    }

    protected ResultObject badInput(RequestObject request) {
        ResultObject result = new ResultObject();
        result.setTransformStatus(BAD_INPUT);
        return result;
    }

    protected ResultObject validateInstantaneousData(RequestObject request, int initialCount, int deletedCount, int affectedCount) {
        ResultObject result = new ResultObject();
        int expectedAffectedCount = timeSeriesDao.doGetInstantaneousExpectedPoints(request);
        int actualFinalCount = timeSeriesDao.doGetInstantaneousCount(request.getUniqueId());

        result.setTransformStatus(determineStatus(
                initialCount,
                deletedCount,
                affectedCount,
                actualFinalCount,
                expectedAffectedCount));
        result.setAffectedTimeSteps(affectedCount);
        result.setDeletedTimeSteps(deletedCount);
        result.setTotalTimeSteps(actualFinalCount);

        return result;
    }

    protected ResultObject validateTsDescriptionListInstantaneous(RequestObject request, int initialCount, int affectedCount) {
        ResultObject result = new ResultObject();
        int actualFinalCount = timeSeriesDao.doGetInstantaneousCount(request.getUniqueId());

        if (initialCount == affectedCount && affectedCount == actualFinalCount) {
            result.setTransformStatus(SUCCESSFUL);
        } else {
            result.setTransformStatus(TRANSFORM_ERROR);
        }
        result.setAffectedTimeSteps(affectedCount);
        result.setDeletedTimeSteps(0);
        result.setTotalTimeSteps(actualFinalCount);

        return result;
    }

    protected String determineStatus(int initialCount, int deletedCount, int affectedCount,
                                     int actualFinalCount, int expectedAffectedCount) {
        if ((initialCount - deletedCount + affectedCount) == actualFinalCount
                && affectedCount == expectedAffectedCount) {
            return SUCCESSFUL;
        } else {
            return TRANSFORM_ERROR;
        }
    }
}
