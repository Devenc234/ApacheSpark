from pyspark.sql import SparkSession

# conf = SparkConf().setAppName("GDP Data Analysis")
# conf = conf.setMaster("local[*]")
# sc   = SparkContext(conf=conf)

spark = SparkSession.builder.appName('Gross domestic product (GDP) Analysis').getOrCreate()

# Reading csv file with having Header info
df=spark.read.csv('/home/zemoso/Desktop/Spark/gdp.csv', mode="DROPMALFORMED",inferSchema=True, header = True)

# Displays the content of the DataFrame to stdout
df.show()

# Renaming the column or use backtick 
df = df.withColumnRenamed("Country Name", "CName")
df = df.withColumnRenamed("Country Code", "CCode")
df.createOrReplaceTempView("GDPdata")

# Calculating GDP
sqlDF1 = spark.sql("SELECT g1.CName, g1.CCode, g1.Year, (g1.Value/g2.Value)-1 AS GDP_Growth FROM GDPdata g1, GDPdata g2 WHERE g1.CName = g2.CName AND g1.Year = g2.Year+1")
sqlDF1.createOrReplaceTempView("GrowthData")

# Finding max GDP
sqlDF2 = spark.sql("SELECT Year, max(GDP_Growth) AS Max_Growth FROM GrowthData GROUP BY Year ORDER BY Year")
sqlDF2.createOrReplaceTempView("MaxGrowthData")

# Result
sqlDF3 = spark.sql("SELECT  gd.Year,gd.CName AS CountryName,gd.CCode AS CountryCode,gd.GDP_Growth FROM GrowthData gd, MaxGrowthData mgd WHERE gd.Year = mgd.Year AND gd.GDP_Growth = mgd.Max_Growth ORDER BY gd.Year")
sqlDF3.show(56)
