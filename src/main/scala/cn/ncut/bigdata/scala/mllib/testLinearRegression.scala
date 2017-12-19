package cn.ncut.bigdata.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object testLinearRegression {

  def main(args: Array[String]): Unit = {
    //1. 构建Spark对象
    val conf = new SparkConf().setMaster("local").setAppName("LinearRegressionWithSGD")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    //读取样本数据1
    val data_path1 = "data/mllib/lpsa.data"
    val data = sc.textFile(data_path1)
    val examples = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    val numExamples = examples.count()

    //2. 读取样本数据2
    val data_path2 = "data/mllib/sample_linear_regression_data.txt"
    val examples2 = MLUtils.loadLibSVMFile(sc, data_path2).cache()

    //3. 新建线性回归模型，并设置训练参数
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 1.0
    val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    println(s"weight = " + model.weights)
    println(s"intercept = " + model.intercept)

    //4. 对样本进行测试
    val prediction = model.predict(examples.map(_.features))
    val predictionAndLable = prediction.zip(examples.map(_.label))
    val print_predict = predictionAndLable.take(50)
    println("prediction " + "\t" + " label")
    for (i <- print_predict.indices) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    //计算测试误差
    val loss = predictionAndLable.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numExamples)
    println(s"Test RMSE = $rmse.")

    //模型保存
    //    val modelPath = ""
    //    model.save(sc,modelPath)
    //    val saveModel = LinearRegressionModel.load(sc,modelPath)
  }

}
