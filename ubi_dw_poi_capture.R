## 预先在HDFS上建文件夹，在hive里面建表(这个操作只要做一次)----------------------------------------------
# 命令模式下里面建文件夹
hadoop fs -mkdir /user/kettle/ubi/dw/poi/poi_prob
# hive里面建表
--建一个外部表，指向/user/kettle/ubi/dw/poi/poi_prob里面的数据
CREATE external TABLE if not exists poi_prob (
  ID String,
  start String,
  type_100 DOUBLE,
  type_101 DOUBLE,
  type_200 DOUBLE,
  type_300 DOUBLE,
  type_500 DOUBLE,
  type_600 DOUBLE,
  type_601 DOUBLE,
  type_604 DOUBLE,
  type_800 DOUBLE,
  type_803 DOUBLE,
  type_806 DOUBLE,
  type_900 DOUBLE,   
  type_901 DOUBLE,
  type_902 DOUBLE,
  type_1000 DOUBLE,
  type_1100 DOUBLE,
  type_1101 DOUBLE,
  type_1200 DOUBLE,
  type_1203 DOUBLE,
  type_1300 DOUBLE,
  type_1301 DOUBLE,
  type_1302 DOUBLE,
  type_1303 DOUBLE,
  type_1304 DOUBLE,
  type_1305 DOUBLE,
  type_1400 DOUBLE,
  type_1403 DOUBLE,
  type_1404 DOUBLE,
  type_1500 DOUBLE,
  type_1501 DOUBLE,
  type_1600 DOUBLE,
  type_1601 DOUBLE,
  type_1800 DOUBLE,
  type_1900 DOUBLE,
  type_2200 DOUBLE,
  type_9900 DOUBLE
)
partitioned BY (stat_date STRING)
ROW format delimited FIELDS TERMINATED BY ','
LOCATION 'hdfs://hadoop-namenode1:8020/user/kettle/ubi/dw/poi/poi_prob';
## 数据读取---------------------------------------------------------------
library("SparkR")
connectBackend.orig <- getFromNamespace('connectBackend', pos='package:SparkR')    
connectBackend.patched <- function(hostname, port, timeout = 3600*48) {
  connectBackend.orig(hostname, port, timeout)
}
assignInNamespace("connectBackend", value=connectBackend.patched, pos='package:SparkR')

sc <- sparkR.init(appName = "ubi_dw_poi_capture")
sqlContext <- sparkRSQL.init(sc)
hiveContext <- sparkRHive.init(sc)
args <- commandArgs(trailing = TRUE)
library("magrittr")
library("data.table")
library("bitops")
library("stringr")
year_month_day <- args[[1]]
# year_month_day <- gsub('-', '', substr(Sys.time(), 1, 10))
SparkR:::includePackage(sqlContext, 'magrittr')
SparkR:::includePackage(sqlContext, 'data.table')
SparkR:::includePackage(sqlContext, 'bitops')
SparkR:::includePackage(sqlContext, 'RCurl')
SparkR:::includePackage(sqlContext, 'stringr')
poi.information.500 <- sql(hiveContext, paste0("select deviceid, actual_start, lon_en_ori, lat_en_ori from trip_stat where trip_stat.stat_date==", year_month_day))
poi.information.500.rdd <- SparkR:::toRDD(poi.information.500) %>% SparkR:::repartition(200L)
SparkR:::cache(poi.information.500.rdd)
## 其他数据准备-----------------------------------------------------------------
name_sort_DF <- read.json(sqlContext, "/user/kettle/ubi/dw/poi/ubi_dw_name_sort.json")
name_sort <- collect(name_sort_DF)
name_I_num <- unique(name_sort[, 1])
num_name_I <- length(name_I_num)
output <-  'xml'
radius <- 500
extensions <- "all"
batch <- 'false'
homeorcorp <- 0                                                                              #爬虫参数
map_ak <- '' # 提供key
k.r <- 6371393     # 地球半径
url_head = paste0("http://restapi.amap.com/v3/geocode/regeo?key=", map_ak, "&location=")
url_tail = paste0("&poitype=&radius=",radius,"&extensions=",extensions,"&batch=", batch,"&homeorcorp=",homeorcorp, "&output=",output, "&roadlevel=1")
broadcast_name_sort <- SparkR:::broadcast(sc, name_sort)
broadcast_url_head <- SparkR:::broadcast(sc, url_head)
broadcast_url_tail <- SparkR:::broadcast(sc, url_tail)
broadcast_name_I_num <- SparkR:::broadcast(sc, name_I_num)

## 爬取---------------------------------------------------------------
prob.rdd <- SparkR:::map(poi.information.500.rdd, function(x){
  library("data.table")
  library("bitops")
  library("RCurl")
  library("stringr")
  name_I_num <- SparkR:::value(broadcast_name_I_num)
  name_sort <- SparkR:::value(broadcast_name_sort)
  url_tail <- SparkR:::value(broadcast_url_tail)
  url_url_head <- SparkR:::value(broadcast_url_head)
  NullPoi <- function(x) x == "pois type=\"list\"/><roads type"
  MergeDistance <- function(x){
    x <- as.numeric(x)
    if(x > 600){
      return(1)
    }else{
      if(x <= 50){
        return(9)
      }else{
        if(x <= 100){
          return(7)
        }else{
          return(floor((700 - x)/100))
        }
      }
    }
  }
  GeturlProbPoi <- function(get_url){
    rule <- gregexpr("pois type=(.*?)<roads type", get_url)
    value <- regmatches(get_url, rule)
    names(value) <- 1
    if(NullPoi(value) == TRUE | length(value[[1]]) == 0){
      poi.lonlat <- data.table(type = 0, distance = 0)      # 判断是否有poi
    } else{
      rule.distance <- gregexpr("distance>(.*?)</distance", value)
      value.distance <- unlist(regmatches(value, rule.distance))
      rule.type <- gregexpr("type>(.*?)</type", value)
      value.type <- unlist(regmatches(value, rule.type))
      num.type.in <- which(lapply(str_locate_all(value.type, ";"), length) == 8)
      if (length(num.type.in) != 0){             # 判断每个type间是不是只有一个POI
        type_replace <- unlist(str_split(str_replace(value.type[num.type.in], "[|]", "</type|type>"), "[|]"))
        value.type <- c(value.type[- num.type.in], type_replace)
        value.distance <- c(value.distance[- num.type.in], unlist(lapply(value.distance[num.type.in], rep, 2)))
      }
      poi.lonlat <- data.table(type = value.type, distance = value.distance)
      poi.lonlat[,`:=`(type = name_sort$num[match(type, name_sort$type)])]
      poi.lonlat <- poi.lonlat[! is.na(type),]         # 选取type有值的行 
      if(nrow(poi.lonlat) == 0){                  # 判断是否有type满足要求的poi
        poi.lonlat <- data.table(type = 0, distance = 0)
      }else{
        poi.lonlat[, `:=`(distance = substring(distance, first = unlist(gregexpr(">", distance)) + 1, 
                                               last = unlist(gregexpr("<", distance)) - 1))]
      }
      rm(rule.distance, value.distance, rule.type, value.type)
    }
    rm(rule, value)
    x <- poi.lonlat[, .(type, distance)]
    # 'x'是一个“data.table"，有'type'和'distance'两列数据
    name_I_num.prob <- data.table(type = name_I_num, prob = 0)
    x <- cbind(data.table(line = 1:nrow(x)), x)
    if(x[, type][1] == 0){       # 判断'x'里面type == 0,即没有POI
      temp <- dcast(name_I_num.prob, .~type, value.var = "prob")[, -1, with = FALSE]
      names(temp) <- paste0("type_", names(temp))
      return(cbind(data.table(deviceid = user_line[1], actual_start = user_line[2]), temp))
      rm(x, poi.lonlat)
    }else{
      x[, prob := MergeDistance(distance), by = line][, prob := prob/sum(prob)]
      x <- x[, lapply(.SD, sum), by = type, .SDcols = "prob"]
      temp <- match(x[, type], name_I_num.prob[, type])
      name_I_num.prob[temp, prob := x[, prob]]
      rm(x, temp, poi.lonlat)
      temp <- dcast(name_I_num.prob, .~type, value.var = "prob")[, -1, with = FALSE]
      names(temp) <- paste0("type_", names(temp))
      return(cbind(data.table(deviceid = user_line[1], actual_start = user_line[2]), temp))
    }
  }
  LonlatPoi <- function(lon, lat){
    url <- paste0(url_head, lon, ",", lat, url_tail)
    get_url <- RCurl:::getURL(url)
    return(GeturlProbPoi(get_url))
  }
  user_line <- matrix(unlist(x), nrow = 1) 
  LonlatPoi(user_line[3], user_line[4])
})

## 存储---------------------------------------------------------------
SparkR:::saveAsTextFile(prob.rdd, paste0("/user/kettle/ubi/dw/poi/poi_prob/stat_date=", year_month_day))
# 给hive里面的表poi_prob添加分区stat_date=year_month_day。
sql(hiveContext, paste0("ALTER TABLE poi_prob ADD IF NOT EXISTS PARTITION(stat_date=", year_month_day, paste0(") LOCATION '/user/kettle/ubi/dw/poi/poi_prob/stat_date=", year_month_day, "'")))
sparkR.stop()
