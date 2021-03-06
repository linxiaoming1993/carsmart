library("SparkR")
connectBackend.orig <- getFromNamespace('connectBackend', pos='package:SparkR')
connectBackend.patched <- function(hostname, port, timeout = 3600*48) {
  connectBackend.orig(hostname, port, timeout)
}
assignInNamespace("connectBackend", value=connectBackend.patched, pos='package:SparkR')
sc <- sparkR.init(appName = "ubi_dm_poi_scores")
sqlContext <- sparkRSQL.init(sc)
hiveContext <- sparkRHive.init(sc)
args <- commandArgs(trailing = TRUE)
library("magrittr")
library("data.table")
library("stringr")
library("rjson")
year_month <- args[[1]] %>% as.character()

# 选取需要的数据并去除停留时间小于0的行程
poi.information.500 <- sql(hiveContext, "select deviceid, tid, vid, actual_start, s_end, dura, lon_en_ori, lat_en_ori, sort_st, sort_en, stat_date from ubi_dw_cluster_point")
poi.information.500 <- filter(poi.information.500, poi.information.500$stat_date == year_month)
poi.information.500 <- poi.information.500 %>% withColumnRenamed("lon_en_ori", "lon") %>%
  withColumnRenamed("lat_en_ori", "lat") %>%
  withColumnRenamed("dura", "Dura")
registerTempTable(poi.information.500, "poi")
query <- "SELECT * , LEAD(actual_start, 1, 0) OVER (PARTITION BY deviceid ORDER BY actual_start) AS start2 FROM poi"
poi.information.500 <- sql(hiveContext, query)
poi.information.500 <- withColumn(poi.information.500, "Dura2", poi.information.500$start2 - poi.information.500$s_end) %>%
  select(c("deviceid", "tid", "vid", "actual_start", "lon", "lat", "Dura", "Dura2", "sort_st", "sort_en"))
poi.information.500 <- filter(poi.information.500, poi.information.500$Dura2 > 0)

#选取出发点和到达点不同时为家和公司的行程
home_company <- sql(hiveContext, "SELECT deviceid, tid, vid, home1, home2, company1, company2, stat_date FROM ubi_dm_address_recognition")
home_company <- filter(home_company, home_company$stat_date == paste0(year_month, "01"))
home_company <- withColumnRenamed(home_company, "deviceid", "ID")
home_company <- withColumnRenamed(home_company, "vid", "V_ID")
home_company <- withColumnRenamed(home_company, "tid", "T_ID")
poi.information.500 <- join(poi.information.500, home_company, poi.information.500$deviceid == home_company$ID)
poi.information.500 <- filter(poi.information.500, poi.information.500$sort_en != poi.information.500$home1 & poi.information.500$sort_en != poi.information.500$home2
                              & poi.information.500$sort_en != poi.information.500$company1 & poi.information.500$sort_en != poi.information.500$company2)
poi.information.500 <- poi.information.500[, c("deviceid", "tid", "vid", "actual_start", "lon", "lat", "Dura", "Dura2")]

name_sort_DF <- read.json(sqlContext, "/user/kettle/ubi/dw/poi/ubi_dw_name_sort.json")
name_sort <- collect(name_sort_DF)
name_I_num <- unique(name_sort[, 1])

prob <- sql(hiveContext, "select * from poi_prob")
prob <- mutate(prob, s = substr(prob$stat_date, 1, 6))
prob <- filter(prob, prob$s == year_month)
prob$s <- NULL
prob <- mutate(prob, t = substr(prob$start, 3, 15))
prob$start <- NULL
prob <- withColumnRenamed(prob, "t", "start")
prob$type_200 <- NULL
prob$type_1300 <- NULL
prob$type_1301 <- NULL
prob$type_1303 <- NULL
prob$type_1304 <- NULL
prob$type_1305 <- NULL
prob$type_1501 <- NULL
prob$type_1900 <- NULL
prob$type_2200 <- NULL
prob$type_9900 <- NULL
prob$type_1000 <- NULL

poi.information <- join(poi.information.500, prob, poi.information.500$deviceid == prob$id & poi.information.500$actual_start == prob$start)
#------------------------------------------------------------------------
# 由poi.information计算poi的次数、驶入时间、停留时间。
poi.information <- mutate(poi.information, Dura_100 = poi.information$Dura * poi.information$type_100,
                          Dura_101 = poi.information$Dura * poi.information$type_101,
                          Dura_300 = poi.information$Dura * poi.information$type_300,
                          Dura_500 = poi.information$Dura * poi.information$type_500,
                          Dura_600 = poi.information$Dura * poi.information$type_600,
                          Dura_601 = poi.information$Dura * poi.information$type_101,
                          Dura_604 = poi.information$Dura * poi.information$type_604,
                          Dura_800 = poi.information$Dura * poi.information$type_800,
                          Dura_803 = poi.information$Dura * poi.information$type_803,
                          Dura_806 = poi.information$Dura * poi.information$type_806,
                          Dura_900 = poi.information$Dura * poi.information$type_900,
                          Dura_901 = poi.information$Dura * poi.information$type_901,
                          Dura_902 = poi.information$Dura * poi.information$type_902,
                          # Dura_1000 = poi.information$Dura * poi.information$type_1000,
                          Dura_1100 = poi.information$Dura * poi.information$type_1100,
                          Dura_1101 = poi.information$Dura * poi.information$type_1101,
                          Dura_1200 = poi.information$Dura * poi.information$type_1200,
                          Dura_1203 = poi.information$Dura * poi.information$type_1203,
                          Dura_1302 = poi.information$Dura * poi.information$type_1302,
                          Dura_1400 = poi.information$Dura * poi.information$type_1400,
                          Dura_1403 = poi.information$Dura * poi.information$type_1403,
                          Dura_1404 = poi.information$Dura * poi.information$type_1404,
                          Dura_1500 = poi.information$Dura * poi.information$type_1500,
                          Dura_1600 = poi.information$Dura * poi.information$type_1600,
                          Dura_1601 = poi.information$Dura * poi.information$type_1601,
                          Dura_1800 = poi.information$Dura * poi.information$type_1800,
                          Dura2_100 = poi.information$Dura2 * poi.information$type_100,
                          Dura2_101 = poi.information$Dura2 * poi.information$type_101,
                          Dura2_300 = poi.information$Dura2 * poi.information$type_300,
                          Dura2_500 = poi.information$Dura2 * poi.information$type_500,
                          Dura2_600 = poi.information$Dura2 * poi.information$type_600,
                          Dura2_601 = poi.information$Dura2 * poi.information$type_101,
                          Dura2_604 = poi.information$Dura2 * poi.information$type_604,
                          Dura2_800 = poi.information$Dura2 * poi.information$type_800,
                          Dura2_803 = poi.information$Dura2 * poi.information$type_803,
                          Dura2_806 = poi.information$Dura2 * poi.information$type_806,
                          Dura2_900 = poi.information$Dura2 * poi.information$type_900,
                          Dura2_901 = poi.information$Dura2 * poi.information$type_901,
                          Dura2_902 = poi.information$Dura2 * poi.information$type_902,
                          # Dura2_1000 = poi.information$Dura2 * poi.information$type_1000,
                          Dura2_1100 = poi.information$Dura2 * poi.information$type_1100,
                          Dura2_1101 = poi.information$Dura2 * poi.information$type_1101,
                          Dura2_1200 = poi.information$Dura2 * poi.information$type_1200,
                          Dura2_1203 = poi.information$Dura2 * poi.information$type_1203,
                          Dura2_1302 = poi.information$Dura2 * poi.information$type_1302,
                          Dura2_1400 = poi.information$Dura2 * poi.information$type_1400,
                          Dura2_1403 = poi.information$Dura2 * poi.information$type_1403,
                          Dura2_1404 = poi.information$Dura2 * poi.information$type_1404,
                          Dura2_1500 = poi.information$Dura2 * poi.information$type_1500,
                          Dura2_1600 = poi.information$Dura2 * poi.information$type_1600,
                          Dura2_1601 = poi.information$Dura2 * poi.information$type_1601,
                          Dura2_1800 = poi.information$Dura2 * poi.information$type_1800)   # 每列同时乘以Dura，Dura2，计算每次行程各POI的Dura,Dura2
poi.information <- mutate(poi.information, N = lit(1))
poi.information.id <- sum(groupBy(poi.information, "deviceid", "tid", "vid"))
poi.information.id.rdd <- SparkR:::toRDD(poi.information.id)
SparkR:::cache(poi.information.id)
names_poi <- name_I_num[- match(c(200, 1300, 1301, 1303, 1304, 1305, 1501, 1900, 2200, 9900, 1000), name_I_num)]
broadcast_names_poi <- SparkR:::broadcast(sc, names_poi)
year_month <- args[[1]]
broadcast_month <- SparkR:::broadcast(sc, year_month)
SparkR:::includePackage(sqlContext, 'data.table')
scores.rdd <- SparkR:::map(poi.information.id.rdd, function(x){
  library("data.table")
  names_poi <- SparkR:::value(broadcast_names_poi)
  p <- data.table(poi_dim_id = 1:25, percent = 0)
  user <- matrix(unlist(x), ncol = 81)
  u <- data.table(imei = user[1, 1], tid = user[1, 2], vid = user[1, 3], dim_month = SparkR:::value(broadcast_month))
  user1 <- as.numeric(user[1, -1])
  user1[80] <- sum(user1[5:29])
  user1[3] <- sum(user1[30:54])
  user1[4] <- sum(user1[55:79])
  user1[5:29] <- user1[5:29] / user1[80]
  user1[30:54] <- user1[30:54] / user1[3]
  user1[55:79] <- user1[55:79] / user1[4]
  temp <- which(user1[5:29] != 0)
  user1 <- user1[c(temp, temp + 25, temp + 50) + 4]
  default_ratio <- c(0.5755, 0.0555, 0.3689)
  options(scipen = 5)
  if(length(temp) == 0){
    out <- cbind(u, p[1, ])
  }else{
    if(length(temp) == 1){
      p$percent[temp] <- 1
      out <- cbind(u, p[temp, ])
    }
    else{
      user1 <- user1 * c(rep(default_ratio[1], length(temp)), rep(default_ratio[2], length(temp)), rep(default_ratio[3], length(temp)))
      x <- matrix(user1, ncol = 3)
      if(any(is.na(var(x)))){
        TDDweights <- c(1/3, 1/3, 1/3)
      }else{
        x.eigen <- eigen(var(x))
        TDDweights <- abs(apply(t(x.eigen[[2]]) * x.eigen[[1]], 2, sum) / sum(x.eigen[[1]]))
        TDDweights <- TDDweights / sum(TDDweights)
      }
      prob.temp <- x[, 1] * TDDweights[1] + x[, 2] * TDDweights[2] +x[, 3] * TDDweights[3]
      p$percent[temp] <- ceiling(prob.temp / sum(prob.temp) * 10000) / 10000
      out <- cbind(u, p[temp, ])
    }
  }
  out <- out[order(- percent)]
  return(out)
})
scores.rdd.result <- SparkR:::flatMap(scores.rdd, function(x){
  user <- matrix(unlist(x), ncol = 6, byrow = F)
  return(split(user, row(user)))
})
scores.rdd.result <- SparkR:::filterRDD(scores.rdd.result, function(x) as.numeric(x[6]) != 0)
SparkR:::saveAsTextFile(scores.rdd.result, paste0("/user/kettle/ubi/dm/ubi_dm_poi_scores/stat_date=", year_month, "01"))
## 给hive表添加分区数据-----------------------------------------------------
sql(hiveContext, "DROP TABLE IF EXISTS temp_poi_scores")
sql(hiveContext, paste0("CREATE external TABLE temp_poi_scores(imei STRING,tid STRING,vid STRING,dim_month STRING,poi_dim_id STRING,percent STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'hdfs://hadoop-namenode1:8020/user/kettle/ubi/dm/ubi_dm_poi_scores/stat_date=", year_month, "01'"))
sql(hiveContext, "insert overwrite table temp_poi_scores select trim(imei) as imei,trim(tid) as tid,trim(vid) as vid,trim(dim_month) as dim_month,trim(poi_dim_id) as poi_dim_id,trim(percent) as percentfrom temp_poi_scores;")
sql(hiveContext, paste0("ALTER TABLE ubi_dm_poi_scores ADD IF NOT EXISTS PARTITION(stat_date=", year_month, "01) LOCATION ", paste0("'/user/kettle/ubi/dm/ubi_dm_poi_scores/stat_date=", year_month, "01'")))
sparkR.stop()

