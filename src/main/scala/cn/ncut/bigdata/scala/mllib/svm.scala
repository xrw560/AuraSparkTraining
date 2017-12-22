package cn.ncut.bigdata.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhouning on 2017/12/22.
  * desc: 
  */
object svm {

  def main(args: Array[String]): Unit = {

    //1 构建Spark 对象
    val conf = new SparkConf().setMaster("local").setAppName("svm")
    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.WARN)

    //2 读取样本数据，为LIBSVM格式
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    //3 样本数据划分训练样本与测试样本
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    //4 新建支持向量机模型，并训练
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    //5 对测试样本进行测试
    val predictionAndLabel = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- print_predict.indices) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    //6 误差计算
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("Area under ROC = " + accuracy)


    //7 保存模型
  }

}
