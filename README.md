# Aquarius Timeseries (AQTS) Daily Values Transformation

[![Build Status](https://travis-ci.com/usgs/aqts-capture-dvstat-transform.svg?branch=master)](https://travis-ci.com/usgs/aqts-capture-dvstat-transform)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-dvstat-transform/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-ts-type-router)

Transform the requested Daily Values timeseries data.

## Testing
This project contains JUnit tests. Maven can be used to run them (in addition to the capabilities of your IDE).

### Docker Network
A named Docker Network is needed to run the automated tests via maven. The following is a sample command for creating your own local network. In this example the name is aqts and the ip addresses will be 172.25.0.x

```.sh
docker network create --subnet=172.25.0.0/16 aqts
```

### Unit Testing
To run the unit tests of the application use:

```.sh
mvn package
```

### Database Integration Testing
To additionally start up a Docker database and run the integration tests of the application use:

```.sh
mvn verify -DTESTING_DATABASE_PORT=5437 -DTESTING_DATABASE_ADDRESS=localhost -DTESTING_DATABASE_NETWORK=aqts -DROOT_LOG_LEVEL=INFO
```

### LocalStack
TBD