from __future__ import print_function
import pyspark
import time
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.sql.functions import *



def accuracy(rawData):
    acc=float(rawData.filter("is_correct = 'true'").count())/float(rawData.count())
    print("total accuracy=",acc)
    return acc

def specific_problem_number_accuracy(rawData,problem_number):
    sel=rawData.select("problem_number","is_correct")
    total=sel.filter("problem_number = '{:.0f}'".format(problem_number))
    correct=total.filter("is_correct = 'true'")
    acc=float(correct.count())/float(total.count())
    print("problem number=",problem_number, " accuracy=",acc)

def accuracy_with_difficulty(rawData):
    sel=rawData.select("problem_number","is_correct","difficulty")
    hard=sel.filter("difficulty = 'hard'")
    hard_correct=hard.filter("is_correct = 'true'")
    hard_acc=float(hard_correct.count())/float(hard.count())
    print("hard accuracy=",hard_acc)

    sel=rawData.select("problem_number","is_correct","difficulty")
    normal=sel.filter("difficulty = 'normal'")
    normal_correct=normal.filter("is_correct = 'true'")
    normal_acc=float(normal_correct.count())/float(normal.count())
    print("normal accuracy=",normal_acc)

    sel=rawData.select("problem_number","is_correct","difficulty")
    easy=sel.filter("difficulty = 'easy'")
    easy_correct=easy.filter("is_correct = 'true'")
    easy_acc=float(easy_correct.count())/float(easy.count())
    print("easy accuracy=",easy_acc)

    sel=rawData.select("problem_number","is_correct","difficulty")
    unset=sel.filter("difficulty = 'unset'")
    unset_correct=unset.filter("is_correct = 'true'")
    unset_acc=float(unset_correct.count())/float(unset.count())
    print("unset accuracy=",unset_acc)
    acc_diff=[hard_acc,normal_acc,easy_acc,unset_acc]
    return acc_diff

def accuracy_each_grade(rawData):
    grade_acc=[]
    for i in range(12):
        sel=rawData.select("user_grade","is_correct")
        temp=sel.filter("user_grade = '{:.0f}'".format(i+1))
        temp_correct=temp.filter("is_correct = 'true'")
        temp_acc=float(temp_correct.count())/float(temp.count())
        grade_acc.append(temp_acc)
        print("grade ",i+1," accuracy=",temp_acc)
    return grade_acc

def F(rawData):
    # num of problem each student did
    x=rawData.select("uuid","problem_number").groupBy("uuid").agg(countDistinct("problem_number"))
    x.show()
    x.agg({'count(DISTINCT problem_number)':'mean'}).show()
    x.coalesce(1).write.option("sep",",").mode("overwrite").csv("gs://r09922114-bucket/num of problem each student did")

def G(rawData):
    # acc of different problem
    sel=rawData.select("problem_number","content_pretty_name","is_correct")
    sel=sel.withColumn("is_correct", sel["is_correct"].cast("int"))
    x=sel.groupBy("problem_number").agg({'is_correct':'mean'})
    x=x.sort(desc("problem_number"))
    x=x.sort(desc("avg(is_correct)"))
    x.show()
    x.coalesce(1).write.option("sep",",").mode("overwrite").csv("gs://r09922114-bucket/acc of different problem")

    #y=rawData.select("problem_number","content_pretty_name")
    #z=x.join(y, on='problem_number', how='inner')
    #z.coalesce(1).write.option("sep",",").mode("overwrite").csv("gs://r09922114-bucket/acc of different problem")
    #z.show()

start_time = time.time()
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("train_model")\
        .getOrCreate()

    # import the csv files as dataFrames
    Log_Problem = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Log_Problem.csv")
    Info_Content = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Info_Content.csv")
    Info_UserData = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Info_UserData.csv")

    rawData = Log_Problem.join(Info_UserData, on='uuid', how='inner')\
        .join(Info_Content, on='ucid', how='inner')
    acc=accuracy(rawData)
    specific_problem_number_accuracy(rawData,18)
    accuracy_with_difficulty(rawData)
    accuracy_each_grade(rawData)
    F(rawData)
    G(rawData)
    print("--- %s seconds ---" % (time.time() - start_time))




    