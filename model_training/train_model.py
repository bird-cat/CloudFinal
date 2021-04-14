from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("train_model")\
        .config("spark.debug.maxToStringFields", 1000)\
        .getOrCreate()
    
    # import the csv files as dataFrames
    Log_Problem = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Log_Problem.csv")
    Info_Content = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Info_Content.csv")
    Info_UserData = spark.read.options(header='True', inferSchema = 'True')\
        .csv("gs://r09922114-bucket/Info_UserData.csv")
    
    # Preprocessing
    # Merge three dataFrames by id
    rawData = Log_Problem.join(Info_UserData, on='uuid', how='inner')\
        .join(Info_Content, on='ucid', how='inner')

    # Feature selection
    features = ["points", "badges_cnt", "user_grade", "user_city",\
                "has_teacher_cnt", "belongs_to_class_cnt", "has_class_cnt", "is_self_coach",\
                "has_student_cnt", "ucid", "difficulty", "learning_stage",\
                "problem_number", "exercise_problem_repeat_session", "total_attempt_cnt", "level"]

    categoryCols = ["user_city", "is_self_coach", "ucid", "difficulty", "learning_stage"]

    numericCols = ["points", "badges_cnt", "user_grade", "has_teacher_cnt",\
                   "belongs_to_class_cnt", "has_class_cnt", "has_student_cnt", "problem_number",\
                   "exercise_problem_repeat_session", "total_attempt_cnt", "level"]

    lrData = rawData.select(col("is_correct").alias("label"), *features)
    lrData = lrData.withColumn("label", lrData["label"].cast("float")).withColumn("is_self_coach", lrData["is_self_coach"].cast("int"))
    
    # Get training and testing data
    training, test = lrData.randomSplit([0.6, 0.4], seed=1126)
    
    # Build pipeline
    indexers = [StringIndexer(inputCol=categoryCol, outputCol="{0}_indexed".format(categoryCol), handleInvalid="keep") for categoryCol in categoryCols]
    encoders = [OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol="{0}_encoded".format(indexer.getOutputCol())) for indexer in indexers]
    assembler = VectorAssembler(inputCols=numericCols+[encoder.getOutputCol() for encoder in encoders], outputCol="unscaled_features")
    scaler = StandardScaler(inputCol="unscaled_features", outputCol="features")
    lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=1000)
    pipeline = Pipeline(stages = indexers + encoders + [assembler, scaler, lr])
    
    
    # Cross validation
    paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()

    crossval = CrossValidator(estimator=pipeline,
                        estimatorParamMaps=paramGrid,
                        evaluator=BinaryClassificationEvaluator(),
                        numFolds=5)

    # Train model
    cvModel = crossval.fit(training)

    # Save model
    model = cvModel.bestModel
    model.write().overwrite().save("/home/tupolev4/final_project/model")

    # Make predictions on test data
    prediction = cvModel.transform(test)

    # Error estimation
    rows = prediction.count()
    correctAnswers = prediction.filter(col("label")==1.0).count()
    predictCorrectAnswers = prediction.filter(col("prediction")==1.0).count()
    incorrectAnswers = prediction.filter(col("label")==0.0).count()
    predictIncorrectAnswers = prediction.filter(col("prediction")==0.0).count()
    corrects = prediction.filter(col("label")==col("prediction")).count()
    error = 1.0 - float(corrects) / rows

    print("# Data = %d" % (rows))
    print("# Correct answer = %d" % (correctAnswers))
    print("# Predicted correct answer = %d" % (predictCorrectAnswers))
    print("# Incorrect answer = %d" % (incorrectAnswers))
    print("# Predicted incorrect answer = %d" % (predictIncorrectAnswers))
    print("# Correct = %d" % (corrects))
    print("Test Error = %f" % (error))

    spark.stop()