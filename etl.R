library(RMySQL)
library(RCurl)
library(rjson)
library(lubridate)

conn <- dbConnect(MySQL(), dbname = "log_etl", user = "root", password = "123456", host = "34.77.236.243", port = 3306)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")
dbSendQuery(conn, "reset master;")

# create table
dbSendStatement(conn, 'create table if not exists file
(
	id int auto_increment
		primary key,
	type varchar(20) null,
	uri varchar(100) null,
	size_bucket int null
)')
dbSendStatement(conn, 'create table if not exists log_fact
(
	id int auto_increment
		primary key,
	file_id int null,
	request_id int null,
	time_id int null,
	user_id int null,
	bytes int null,
	time_taken int null
);')
dbSendStatement(conn, 'create table if not exists request
(
    id int auto_increment
    primary key,
    status_code varchar(10) null,
    time_bucket int null,
    success int null
);')
dbSendStatement(conn, 'create table if not exists time
(
	id int auto_increment
		primary key,
	year int null,
	quarter int null,
	month int null,
	date date null,
	time time null,
	day_week int null,
	hour_day int null
);')
dbSendStatement(conn, 'create table if not exists user
(
    id int auto_increment
    primary key,
    ip varchar(20) null,
    browser varchar(20) null,
    os varchar(20) null
);')

# Get the whole table from logs
table <- NULL
col.name <- c("date", "time", "s-ip", "cs-method", "cs-uri-stem", "cs-uri-query",
              "s-port", "cs-username", "c-ip", "cs(User-Agent)", "cs(Cookie)", "cs(Referer)",
              "sc-status", "sc-substatus", "sc-win32-status", "sc-bytes", "cs-bytes", "time-taken")
for (i in dir("W3SVC1")) {
  temp <- read.table(paste0("W3SVC1/", i))
  if (length(temp) == 14) {
    temp <- cbind(temp[, 1:10], a = "-", b = "-", temp[, 11:13], c = "-", d = "-", temp[, 14])
  }
  names(temp) <- col.name
  table <- rbind(table, temp)
}

# insert request data
getSuccess <- function(status.code) {
  if (status.code < 400) {
    1
  }else {
    0
  }
}

getTimeBucket <- function(time.taken) {
  if (time.taken >= 10 && time.taken < 20) {
    10
  }else if (time.taken >= 20 && time.taken < 50) {
    20
  }else if (time.taken >= 50 && time.taken < 100) {
    50
  }else if (time.taken >= 100 && time.taken < 200) {
    100
  }else if (time.taken >= 200 && time.taken < 500) {
    200
  }else if (time.taken >= 500 && time.taken < 1000) {
    500
  }else if (time.taken >= 1000 & time.taken < 2000) {
    1000
  }else if (time.taken >= 2000 && time.taken < 5000) {
    2000
  }else {
    0
  }

}

dbWriteTable(conn, "request", data.frame(status_code = table$`sc-status`,
                                         success = sapply(table$`sc-status`, getSuccess),
                                         time_bucket = sapply(table$`time-taken`, getTimeBucket)), row.names = F, append = T)

#insert time data
dbWriteTable(conn, "time", data.frame(date = table$date,
                                      time = table$time,
                                      year = year(table$date),
                                      month = month(table$date),
                                      quarter = quarter(table$date),
                                      day_week = wday(table$date),
                                      hour_day = hour(as.POSIXct(table$time, format = "%H:%M:%OS"))), row.names = F, append = T)

# insert user data
getOS <- function(OS) {
  if (grepl("Windows", OS)) {
    "Windows"
  }else if (grepl("Macintosh", OS)) {
    "Macintosh"
  }else if (OS == "-") {
    "OS unknown"
  }else {
    "Other"
  }
}

getBrowser <- function(browser) {
  if (grepl("MSIE", browser)) {
    "MSIE"
  }else if (grepl("Firefox", browser)) {
    "Firefox"
  }else if (grepl("msnbot", browser)) {
    "msnbot"
  }else if (grepl("panscient", browser)) {
    "panscient.com"
  }else if (grepl("Baiduspider", browser)) {
    "Baiduspider"
  }else if (grepl("Yandex", browser)) {
    "Yandex"
  }else if (grepl("Safari", browser)) {
    "Safari"
  }else if (grepl("Sosospider", browser)) {
    "Sogou web spider"
  }else if (grepl("compatible", browser)) {
    "Netscape"
  }else {
    "Other"
  }
}

dbWriteTable(conn, "user", data.frame(ip = table$`c-ip`,
                                      os = sapply(table$`cs(User-Agent)`, getOS),
                                      browser = sapply(table$`cs(User-Agent)`, getBrowser)), row.names = F, append = T)

# insert file data
getType <- function(uri) {
  temp <- uri
  type <- strsplit(uri, ".", fixed = T)
  if (substring(temp, nchar(temp)) == "/") {
    "directories"
  }else if (length(type[[1]]) > 1 && !grepl("[^0-9a-zA-Z]", type[[1]][length(type[[1]])])) {
    type[[1]][length(type[[1]])]
  }else {
    "uk"
  }
}

getSizeBucket <- function(bytes) {
  if (bytes != "-") {
    bytes <- as.numeric(bytes)
  }
  if (bytes >= 0 && bytes < 100) {
    0
  }else if (bytes >= 101 && bytes < 1024) {
    101
  }else if (bytes >= 1024 && bytes < 10240) {
    1000
  }else if (bytes >= 10240 && bytes < 102400) {
    10000
  }else if (bytes >= 102400 && bytes < 1048576) {
    100000
  }else {
    -1
  }
}

dbWriteTable(conn, "file", data.frame(uri = table$`cs-uri-stem`,
                                      type = sapply(table$`cs-uri-stem`, getType),
                                      size_bucket = sapply(table$`sc-bytes`, getSizeBucket)), row.names = F, append = T)

# insert fact table data
getBytes <- function(bytes) {
  if (bytes == "-") {
    -1
  }else {
    bytes
  }
}

dbWriteTable(conn, "log_fact", data.frame(file_id = seq(1, nrow(table)),
                                          request_id = seq(1, nrow(table)),
                                          user_id = seq(1, nrow(table)),
                                          time_id = seq(1, nrow(table)),
                                          bytes = sapply(table$`sc-bytes`, getBytes),
                                          time_taken = table$`time-taken`), row.names = F, append = T)

# find robots and update user table
dbSendStatement(conn, "update user
set os='Known robots'
where ip in (select *
             from (select distinct ip
                   from log_fact,
                        file,
                        user
                   where file_id = file.id
                     and user_id = user.id
                     and uri = '/robots.txt') as t1);")

dbDisconnect(conn)

# get geo data
# fromJSON(getURL(paste0("http://ip-api.com/json/", ip)))$city

