spark2-submit --master yarn \
 --queue root.dba \
 gen_profile.py
# --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
