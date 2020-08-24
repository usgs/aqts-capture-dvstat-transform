package gov.usgs.wma.waterdata;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

import org.dbunit.dataset.datatype.AbstractDataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonType extends AbstractDataType {
	private static final Logger LOG = LoggerFactory.getLogger(JsonType.class);

	public JsonType() {
		super("json", Types.OTHER, String.class, false);
	}

	@Override
	public Object typeCast(Object value) throws TypeCastException {
		return value.toString();
	}

	@Override
	public Object getSqlValue(int column, ResultSet resultSet) throws SQLException {
		LOG.trace("getSqlValue({}, {})", column, resultSet);
		if (resultSet.wasNull()) {
			return null;
		}
		String resultString = resultSet.getString(column);
		// convert the string into an array of strings for further manipulation
		String[] resultArray = resultString.substring(1, resultString.length()-1).split(", ");
		// sort the array to make sure the order is consistent between test runs
		Arrays.sort(resultArray);
		return Arrays.toString(resultArray);
	}

	@Override
	public void setSqlValue(Object value, int column, PreparedStatement statement) throws SQLException {
		if (value == null) {
			statement.setNull(column, Types.OTHER);
		}
		statement.setObject(column, value.toString(), Types.OTHER);
	}
}
