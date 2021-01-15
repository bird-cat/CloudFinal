from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import FloatType
from pyspark.sql.functions import *
from pyspark.ml import PipelineModel
import sys

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("train_model")\
        .config("spark.debug.maxToStringFields", 1000)\
        .getOrCreate()
    
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # Load model
    model = PipelineModel.load("/home/tupolev4/final_project/model")

    # import the csv file as dataFrame
    Info_Content = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Info_Content.csv")
    Info_UserData = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Info_UserData.csv")
    Log_Problem = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Log_Problem.csv")

    def ith_(v, i):
        try:
            return float(v[i])
        except ValueError:
            return None

    ith = udf(ith_, FloatType())

    results = None
    # Recommend n problems to certain user
    def recommend(uuid, n=10, difficulty={"easy":False, "normal":True, "hard":False}, do_not_repeat=False):
        # Merge table
        lrData = Info_UserData.filter(Info_UserData.uuid==uuid).crossJoin(Info_Content)
        lrData = lrData.withColumn("is_self_coach", lrData["is_self_coach"].cast("int"))
        lrData = lrData.join(Log_Problem, (Log_Problem.uuid==lrData.uuid) & (Log_Problem.ucid==lrData.ucid), how='left')\
                    .drop(Log_Problem["uuid"]).drop(Log_Problem["ucid"])
        lrData = lrData.fillna(0)
        if do_not_repeat:
            lrData = lrData.filter(col("total_attempt_cnt")==0)

        # Make prediction
        prediction = model.transform(lrData)

        # Select proper problems
        prediction = prediction.withColumn("correct_probability", ith("probability", lit(1)))
        proper_problems = spark.createDataFrame(spark.sparkContext.emptyRDD(), prediction.schema)

        if difficulty["easy"]:
            proper_problems = prediction.filter(col("correct_probability") >= 0.75)
        if difficulty["normal"]:
            proper_problems = proper_problems.union(prediction.filter((col("correct_probability") >= 0.50) & (col("correct_probability") < 0.75)))
        if difficulty["hard"]:
            proper_problems = proper_problems.union(prediction.filter(col("correct_probability") < 0.50))

        selected_problems = proper_problems.orderBy(rand()).limit(n)

        return selected_problems.select("uuid", "ucid", "correct_probability", "content_pretty_name")
    
    # Demo
    print("Start!")
    uuids = Info_UserData.select("uuid").rdd.flatMap(lambda x: x).collect()
    for uuid in uuids:
        selected_problems = recommend(uuid, n=30, difficulty={"easy": True, "normal": True, "hard": True}, do_not_repeat=True)
        if not results:
            results = selected_problems
        else:
            results = results.union(selected_problems)

    results.coalesce(1).write.option("sep",",").mode("overwrite").csv("gs://r09922114-bucket/recommend_results")
    spark.stop()