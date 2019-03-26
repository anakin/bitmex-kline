# Golang 获取bitmex k线历史数据

SDK的部分使用的是：
 https://github.com/qct/bitmex-go
 
 **使用方法：**
 
 创建mysql数据库
 
 每个时段数据保存在对应的表中
 
 数据表字段：
 
 ktime,symbol,open,close,high,low,trades,volume,vwap,lastSize,turnover,homeNotional,foreignNotional
 
 进入main.go所在目录
 
 go build
 
 然后后台运行即可