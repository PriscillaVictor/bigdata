

var file=sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")
var valid=file.map(_.split(" ")).filter(_.size==10)
case class log(host:String,timestamp:String,url:String,code:String,bytes:String)
var df = valid.map(x=> log(x(0).toString,x(3).toString,x(6).toString,x(8).toString,x(9).toString)).toDF()


var result1 = df.groupBy($"url").count().orderBy($"count".desc).show(10)

var result2 = df.groupBy(substring($"timestamp",2,14).as("timeFrame") ).count().orderBy($"count".desc).show(5)

var result3 = df.groupBy(substring($"timestamp",2,14).as("timeFrame") ).count().orderBy($"count").show(5)

var result4 = df.groupBy($"code".as("HTTP code")).count().orderBy($"count".desc).show(2)
