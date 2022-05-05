# Car drive session data analysis with Bigquery

Backend microservice for data processing and queries based on Bigquery and Spring Boot.

## Local development

- Configure the environment variable GOOGLE_APPLICATION_CREDENTIALS with the path to valid Google (service) account key file, e.g.

`$ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/project/key/project_id-12345ab315.json`

- Build the project

`$ mvn clean package`

- Run the application

`$ mvn spring-boot:run`

- The application is available under `http://localhost:8081/points/`, the Bigquery from the configured GCP project is used as the data source

## Batch processing and queries

- Test data is expected to be uploaded to the bigquery tables previously (dataset `drivedata`)

- Use the Postman collection `/src/test/resources/trackpoints.postman_collection.json` to trigger batch jobs and queries
  - Order track points by the route
  - Query all track points
  - Get a track point by the order number
  - Get the list of all available POIs
  - Get POIs nearest to the provide coordinates (around 25 km)
