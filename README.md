# carsmart（涉及的有：R，sparkR， hive，linux）
# ubi_dw_poi_capture.R 是爬取一段行程附近的POI点，并判断这次行程去每一个POI的概率。
# ubi_dm_poi_scores.R 是有每一段行程去附近各个POI的概率，以及对每个人家和公司的判断，在删除回家和去公司的行程以后，分析一个人去各个POI的占比。
# hive建表 是一些在hive里对表的操作。（建外部表，分区，去除数据间的空格，载入数据等）
# R从hive读取数据，是用R直接从读取hive里面的外部表的数据，然后处理。
