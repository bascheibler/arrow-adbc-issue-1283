# arrow-adbc-issue-1283

## Set up environment

### Setting up environment variables
- Export `SNOWFLAKE_PASSWORD` and `SNOWFLAKE_USERNAME` environment variables.
- Edit the .env file to set the remaining variables. I'm using an XS warehouse.

### Reproducing issues

Build and start the Docker container to run the tests under the same conditions:
```
docker-compose up
```