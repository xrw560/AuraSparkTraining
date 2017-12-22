package cn.ncut.bigdata.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhouning on 2017/12/22.
  * desc: 
  */
object tmp_naive_bayes {

  def main(args: Array[String]): Unit = {

    //1 构建Spark 对象
    val conf = new SparkConf().setMaster("local").setAppName("naive_bayes")
    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.WARN)

    //2 读取样本数据
    val data = sc.textFile("data/mllib/sample_naive_bayes_data.txt")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    //样本数据划分训练样本与测试样本
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    //新建贝叶斯分类模型模型，并训练,第一个参数为训练数据，第二个参数为平滑参数，默认为1，可改
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    //对测试样本进行测试
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("accuracy: " + accuracy)
    println("Predictionof (0.0, 2.0, 0.0, 1.0): " + model.predict(Vectors.dense(0.0, 2.0, 0.0, 1.0)))

    //保存模型
  }

}
