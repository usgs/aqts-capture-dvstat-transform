package gov.usgs.wma.waterdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

@SpringBootApplication
public class Application {
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	private TimeSeriesDao timeSeriesDao;

	@Bean
	public Function<RequestObject, ResultObject> transform() {
		return new TransformDailyValue(timeSeriesDao);
	}

	@Bean
	public Function<RequestObject, ResultObject> instantaneous() {
		return new TransformInstantaneousValue(timeSeriesDao);
	}
}
