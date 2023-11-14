ARG REPRODUCE_ISSUE
FROM python:3.12 AS reproduce-issue-true

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip && pip install -r requirements.txt
ENV ADBC_SNOWFLAKE_LIBRARY=/usr/local/lib/python3.12/site-packages/adbc_driver_snowflake/libadbc_driver_snowflake.so

# Build adbc_driver_snowflake from source
FROM reproduce-issue-true AS reproduce-issue-false

ENV BRANCH=race-conditions-suck
RUN git clone --depth 1 -b $BRANCH https://github.com/zeroshade/arrow-adbc.git
WORKDIR /app/arrow-adbc/python/adbc_driver_snowflake
RUN pip install -e ../adbc_driver_manager && pip install --no-deps -e .
WORKDIR /app

FROM reproduce-issue-${REPRODUCE_ISSUE} AS final-base
FROM final-base
# print versions just to make sure it's not hardcoded to 0.8.0
RUN pip freeze
CMD ["python3","main.py"]