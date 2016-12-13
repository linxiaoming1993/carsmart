# 通过RJDBC包从hive里面读取数据，数据是200多万行，然后用data.table处理。
library('RJDBC')
library('data.table')
drv <- JDBC(driverClass = "org.apache.hive.jdbc.HiveDriver",
            classPath = list.files("/home/heqiuguo/drivers/impala-jdbc-0.5-2",pattern="jar$", full.names=T, recursive=T),
            identifier.quote="`")
conn <- dbConnect(drv, "jdbc:hive2://10.23.208.17:21050/;auth=noSasl")
month.data <- dbGetQuery(conn, "SELECT * from ubi_dm_poi_scores")
data <- dcast(month.data, imei + tid + vid + dim_month + stat_date~poi_dim_id, value.var = c("percent"))
data[, `:=`(`1` = `1` + `2` + `3`, `5` = `5` + `6` + `7`, `8` = `8` + `9` + `10`, `11` = `11` + `12` + `13`, `14` = `14` + `15`, `19` = `19` + `20` + `21`, `23` = `23` + `24`)]
out <- data[, list(imei, tid, vid, dim_month, `1`, `4`, `5`, `8`, `11`, `14`, `21`, `22`, `23`)]
out[is.na(out)] <- 0
write.csv(out, "./temp_xiaoming/poi_201607.csv")
