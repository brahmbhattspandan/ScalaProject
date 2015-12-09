
import java.io.File
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.util
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.feature.{StandardScaler,Normalizer,ChiSqSelector}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.SQLContext._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.optimization.{L1Updater,SquaredL2Updater}
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD,RidgeRegressionWithSGD,LassoWithSGD}
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.stat.correlation.Covariance
import scala.util
import com.github.nscala_time.time.Imports._


object SimpleSpark {
  /*
   * maps a Stock name to ID
   */
  def readStockNameMap(file: File) = {
    val lines = Source.fromFile(file).getLines().toSeq
    lines.tail.map(line => {
      val cols = line.split(',')
      val value = cols(0).toDouble
      (cols(1), value)
    }).toMap
  }
  /*
   * maps a ID to Stock name
   */
  def readIdtoStockNameMap(file: File) = {
    val lines = Source.fromFile(file).getLines().toSeq
    lines.tail.map(line => {
      val cols = line.split(',')
      val value = cols(0).toDouble
      (value, cols(1))
    }).toMap
  }
 
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  // Aligns the dataset to the appropriate time period
  def trimToRegion(history: Array[(String, DateTime, Double)], start: DateTime, end: DateTime)
  : Array[(String, DateTime, Double)] = {

    var trimmed = history.dropWhile(_._2 < start).takeWhile(_._2 <= end)
    if (trimmed.head._2 != start) {
      trimmed = Array((trimmed.head._1, start, trimmed.head._3)) ++ trimmed
    }
    if (trimmed.last._2 != end) {
      trimmed = trimmed ++ Array((trimmed.last._1, end, trimmed.last._3))
    }
    trimmed
  }

  def dataMatrix(histories: Array[(String, Double)],  stockNameMap : Map[String,Double]): Array[Double] = {
    val mat = new Array[Double](histories.length)
    //test = test + 1
    //val fileID = test
    var i = 0
    for (y <- histories) {
      if (i < histories.length) {
        mat(i) = y._2.toDouble
        i = (i + 1)
      }
    }
    mat :+ stockNameMap.get(histories(0)._1).get
  }

  // Fills missing data with values from nearest neighbor’s
  def fillInHistory(history: Array[(String, DateTime, Double)], start: DateTime, end: DateTime)
  : Array[(String, DateTime, Double)] = {
    var cur = history
    val filled = new ArrayBuffer[(String, DateTime, Double)]()
    var curDate = start
    while (curDate < end) {
      if (cur.tail.nonEmpty && cur.tail.head._2 == curDate) {
        cur = cur.tail
      }
      filled += ((cur.head._1, curDate, cur.head._3))
      val a = 2.months
      curDate += 1.days
      // Skip weekends
      if (curDate.dayOfWeek().get > 5) curDate += 2.days
    }
    filled.toArray
  }
  /*
   * Reads a Stock history from each file
   */
  def readStockHistory(file: File): Array[(String, DateTime, Double)] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val lines = Source.fromFile(file).getLines().toSeq
    val name = file.getName().split('.')
    lines.tail.map(line => {
      val cols = line.split(',')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (name(0), date, value)
    }).reverse.toArray
  }
  /*
    * Reads a Factor history from each file
    */
  def readFactorHistory(file: File): Array[(DateTime, Double)] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val lines = Source.fromFile(file).getLines().toSeq
    lines.tail.map(line => {
      val cols = line.split(',')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value)
    }).reverse.toArray
  }
  /**
    * Reads a all the Stock histories
    */
  def readAllStockHistories(dir: File): Seq[Array[(String, DateTime, Double)]] = {
    val files = dir.listFiles()
    files.flatMap(file => {
      try {
        Some(readStockHistory(file))
      } catch {
        case e: Exception => None
      }
    })
  }

  def valueReturns(history: Array[(String, DateTime, Double)]): Array[(String, Double)] = {
    history.sliding(10).map { window =>
      val next = window.last._3
      val prev = window.head._3
      (window.head._1, ((next - prev) / prev))
    }.toArray
  }
  //--------------------------------------------------------------------------------------------
  def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](histories.head.length)
    for (i <- 0 until histories.head.length) {
      mat(i) = histories.map(_ (i)).toArray
    }
    mat
  }
  //-----------------
  def dataModel(instrument: Array[Double], factorMatrix: Array[Array[Double]])= {
    val mat = new Array[Array[Double]](instrument.length-1)
    for (i <- 0 until instrument.length-1 ) {
      if (i < instrument.length-1){
        mat(i) = factorMatrix(i):+instrument(i):+instrument(instrument.length-1)
      }

    }
    mat
  }
  //--------------------
  def featurize(factorReturns: Array[Double]): Array[Double] = {
    val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
    val squareRootedReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
    squaredReturns ++ squareRootedReturns ++ factorReturns
  }
  //---------------------------
  def computefactorWeights(dataset:Array[Array[Double]], algoNAme:String, sc:SparkContext)= {
    val stockID = dataset(0)(dataset(0).length-1)
    val data2 = dataset.map{ line =>
      LabeledPoint(line(line.length-2), Vectors.dense(line.dropRight(2)))
    }
    val data2RDD = sc.parallelize(data2)
    if (algoNAme == "LinearRegressionWithSGD_L0"){
      var lrAlg = new LinearRegressionWithSGD()
      lrAlg.optimizer.setNumIterations(100).setStepSize(0.001)
      lrAlg.setIntercept(true)
      val model = lrAlg.run(data2RDD)
      var factorWeights = new Array[Double](model.weights.toArray.length)
      var i = 0
      for (wts <- model.weights.toArray){
        if (i < model.weights.toArray.length){
          factorWeights(i) = wts
          i = i + 1
        }
      }
      println(model.intercept)
      factorWeights = factorWeights:+model.intercept
      factorWeights:+stockID
    }
    if (algoNAme == "LinearRegressionWithSGD_L1"){
      var  lrAlg = new LinearRegressionWithSGD()
      lrAlg.optimizer.setNumIterations(100).setUpdater(new L1Updater).setStepSize(0.001)
      lrAlg.setIntercept(true)
      val model = lrAlg.run(data2RDD)
      var factorWeights = new Array[Double](model.weights.toArray.length)
      var i = 0
      for (wts <- model.weights.toArray){
        if (i < model.weights.toArray.length){
          factorWeights(i) = wts
          i = i + 1
        }
      }
      factorWeights = factorWeights:+model.intercept
      factorWeights:+stockID
    }
    if (algoNAme == "LinearRegressionWithSGD_L2"){
      var lrAlg = new LinearRegressionWithSGD()
      lrAlg.optimizer.setNumIterations(100).setUpdater(new SquaredL2Updater).setStepSize(0.001)
      lrAlg.setIntercept(true)
      val model = lrAlg.run(data2RDD)
      var factorWeights = new Array[Double](model.weights.toArray.length)
      var i = 0
      for (wts <- model.weights.toArray){
        if (i < model.weights.toArray.length){
          factorWeights(i) = wts
          i = i + 1
        }
      }
      factorWeights = factorWeights:+model.intercept
      factorWeights:+stockID
    }
    if (algoNAme == "RidgeRegressionWithSGD"){
      var lrAlg = new RidgeRegressionWithSGD()
      lrAlg.optimizer.setNumIterations(100).setStepSize(0.001)
      lrAlg.setIntercept(true)
      val model = lrAlg.run(data2RDD)
      var factorWeights = new Array[Double](model.weights.toArray.length)
      var i = 0
      for (wts <- model.weights.toArray){
        if (i < model.weights.toArray.length){
          factorWeights(i) = wts
          i = i + 1
        }
      }
      factorWeights = factorWeights:+model.intercept
      factorWeights:+stockID
    }
    if (algoNAme == "LassoWithSGD"){
      // LR default L2
      var  lrAlg = new LassoWithSGD()
      lrAlg.optimizer.setNumIterations(100).setStepSize(0.001)
      lrAlg.setIntercept(true)
      val model = lrAlg.run(data2RDD)
      var factorWeights = new Array[Double](model.weights.toArray.length)
      var i = 0
      for (wts <- model.weights.toArray){
        if (i < model.weights.toArray.length){
          factorWeights(i) = wts
          i = i + 1
        }
      }
      factorWeights = factorWeights:+model.intercept
      factorWeights:+stockID
    }else{
    }
  }
  //-------------------------
  def LinearRegressionWithSGD_L0(dataset:Array[Array[Double]], sc:SparkContext)= {
    val stockID = dataset(0)(dataset(0).length-1)
    val data2 = dataset.map{ line =>
      LabeledPoint(line(line.length-2), Vectors.dense(line.dropRight(2)))
    }
    val data2RDD = sc.parallelize(data2)
    var lrAlg = new LinearRegressionWithSGD()
    lrAlg.optimizer.setNumIterations(100).setStepSize(0.001)
    lrAlg.setIntercept(true)
    val model = lrAlg.run(data2RDD)
    var factorWeights = new Array[Double](model.weights.toArray.length)
    var i = 0
    for (wts <- model.weights.toArray){
      if (i < model.weights.toArray.length){
        factorWeights(i) = wts
        i = i + 1
      }
    }
    factorWeights = factorWeights:+model.intercept
    factorWeights:+stockID
  }
  /*
   * Calculate the return of a particular instrument under particular trial conditions.
   */
  def instrumentTrialReturn(instrument: Array[Double], trial: Array[Double]): Double = {
    var instrumentTrialReturn = instrument(0)
    var i = 0
    while (i < trial.length) {
      instrumentTrialReturn += trial(i) * instrument(i+1)
      i += 1
    }
    instrumentTrialReturn
  }
  /*
   * Calculate the full return of the portfolio under particular trial conditions.
   */
  def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]) = {
    var totalReturn = 0.0
    // var totalR = new Array[(Double,Array[(Double,Double)])] (1)
    val individualTrialReturns = new Array[(Double,Double)](instruments.length)
    var i = 0
    for (instrument <- instruments) {
      val instrumentReturn = instrumentTrialReturn(instrument.dropRight(1), trial)
      totalReturn = totalReturn + instrumentReturn
      if (i < instruments.length ){
        individualTrialReturns(i) = (instrument(instrument.length-1),instrumentReturn)
        i= i+1
      }
    }
    ((totalReturn / instruments.size),individualTrialReturns)
  }
  def trialReturns(
                    seed: Long,
                    numTrials: Int,
                    instruments: Seq[Array[Double]],
                    factorMeans: Array[Double],
                    factorCovariances: Array[Array[Double]]): Array[(Double, Array[(Double, Double)])] = {
    val rand = new MersenneTwister(seed)
    val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans,
      factorCovariances)
    val trialReturns = new Array[(Double,Array[(Double,Double)])](numTrials)
    for (i <- 0 until numTrials) {
      val trialFactorReturns = multivariateNormal.sample()
      val trialFeatures = featurize(trialFactorReturns)
      trialReturns(i) = trialReturn(trialFeatures, instruments)
    }
    trialReturns
  }
  //------------------------------
  def PercentVaR(trials: RDD[Double]): Double = {
    val percentParam = 100 / 10 //financialRiskAnalysisParam.getOrElse(0)
    val topLosses = trials.takeOrdered(math.max(trials.count().toInt / percentParam, 1))
    topLosses.last
  }

  def PercentES(trials: RDD[Double]): Double = {
    val percentParam = 100 / 10 //financialRiskAnalysisParam.getOrElse(0)
    val topLosses = trials.takeOrdered(math.max(trials.count().toInt / percentParam, 1))
    topLosses.sum / topLosses.length
  }

  def confidenceInterval(
                          trials: RDD[Double],
                          computeStatistic: RDD[Double] => Double,
                          numResamples: Int,
                          pValue: Double): (Double, Double) = {
    val stats = (0 until numResamples).map { i =>
      val resample = trials.sample(true, 1.0)
      computeStatistic(resample)
    }.sorted
    val lowerIndex = (numResamples * pValue / 2 - 1).toInt
    val upperIndex = math.ceil(numResamples * (1 - pValue / 2)).toInt
    (stats(lowerIndex), stats(upperIndex))
  }

  def countFailures(stocksReturns: Seq[Array[Double]], valueAtRisk: Double): Int = {
    var failures = 0
    for (i <- 0 until stocksReturns(0).size) {
      val loss = stocksReturns.map(_(i)).sum
      if (loss < valueAtRisk) {
        failures += 1
      }
    }
    failures
  }
  
  def kupiecTestStatistic(total: Int, failures: Int, confidenceLevel: Double): Double = {
    val failureRatio = failures.toDouble / total
    val logNumer = (total - failures) * math.log1p(-confidenceLevel) +
      failures * math.log(confidenceLevel)
    val logDenom = (total - failures) * math.log1p(-failureRatio) +
      failures * math.log(failureRatio)
    -2 * (logNumer - logDenom)
  }

  def BackTestPValue(
                      stocksReturns: Seq[Array[Double]],
                      valueAtRisk: Double,
                      confidenceLevel: Double): Double = {
    val failures = countFailures(stocksReturns, valueAtRisk)
    val total = stocksReturns(0).size
    val testStatistic = kupiecTestStatistic(total, failures, confidenceLevel)
    1 - new ChiSquaredDistribution(1.0).cumulativeProbability(testStatistic)
  }

  def main(args: Array[String]) {
    args.foreach(arg => println("--------------" + arg))
    val conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val financialRiskAnalysisParam = args(0) 
    val nTrials = args(1) 
    val slidingWindow = args(2) 
    val algoNAme = args(3)    
    val filePrefix = "/home/smit/Documents/ScalaProject/FilterData/MapData/StockNamesMap.csv"
    val stockNameMap = readStockNameMap(new File(filePrefix))
    val start = new DateTime(2000, 2, 13, 0, 0)
    val end = new DateTime(2015, 8, 7, 0, 0)
    val rawStocks = readAllStockHistories(new File("/home/smit/Documents/ScalaProject/FilterData/stocks/")).filter(_.size >= 260 * 5 + 10)
    val rawStocksrdd = sc.parallelize(rawStocks)
    rawStocks.length
    val stocks = rawStocksrdd.map(trimToRegion(_, start, end)).map(fillInHistory(_, start, end))
    stocks.count
    val stocksReturns = stocks.map(valueReturns)
    stocksReturns.collect
    val factorsPrefix = "/home/smit/Documents/ScalaProject/FilterData/factors/"
    val factors1 = Array("NDX.csv", "SNP.csv", "ShanghiaCompositeIndex.csv").
      map(x => new File(factorsPrefix + x)).
      map(readStockHistory)
    val fctors = factors1.
      map(trimToRegion(_, start, end)).
      map(fillInHistory(_, start, end))
    val factors = sc.parallelize(fctors)
    val factorsReturns = factors.map(valueReturns)
    val temp1 = factorsReturns.flatMap { arrayElement =>
      arrayElement filter {
        case (x: String, y: Double) => x == "NDX"
      }
    }
    val temp2 = factorsReturns.flatMap { arrayElement =>
      arrayElement filter {
        case (x: String, y: Double) => x == "SNP"
      }
    }
    val factor1 = temp1.map(x => x._2).toArray
    val factor2 = temp2.map(x => x._2).toArray
    temp2.collect.foreach(println)
    val factorMat = factorMatrix(Seq(factor1, factor2))
    val factorMatrdd = sc.parallelize(factorMat)
    val factorFeatures = factorMatrdd.map(featurize)
    factorFeatures.collect
    factorFeatures.count
    var test = 0
    val spantemp = stocksReturns.toArray
    val labels = spantemp.map(i => dataMatrix(i,stockNameMap))
    val tblah = labels.length
    var rnd = new scala.util.Random
    val temp = labels.map(x => dataModel(x, factorFeatures.toArray))
    val t = temp.length
    val factorWts =  temp.map(i => LinearRegressionWithSGD_L0(i, sc))
    val factorCov = new Covariance(factorMat).getCovarianceMatrix().getData()
    val factorMeans = Seq(factor1,factor2).map(factor => factor.sum /factor.size).toArray
    val broadCastFactorWts = sc.broadcast(factorWts)
    val numTrials = args(4).toInt 
    val parallelism = numTrials / 10
    val baseSeed = 1001L 
    baseSeed until baseSeed + parallelism
    val seeds = (baseSeed until baseSeed + parallelism)
    val seedRdd = sc.parallelize(seeds, parallelism)
    var stocksTrialsReturns = scala.collection.mutable.Map[String, Double]()
    val trialsrdd = seedRdd.flatMap(
      trialReturns(_, numTrials / parallelism, broadCastFactorWts.value, factorMeans, factorCov))
    val trials = trialsrdd.map(line => line._1)
    trials.collect
    val stockreturns = trialsrdd.map(line => line._2)
    var j = scala.collection.mutable.Map[Double, Double]()
    val xyz = stockreturns.collect.flatten
    val stocksSummations = xyz.foreach(i => if(j.get(i._1) == None){
      j+= (i._1->i._2)}
    else {
      j+= (i._1 -> (j.get(i._1).get+ i._2))
    })
    val valueAtRisk = PercentVaR(trials)
    val ExpectedShortFall = PercentES(trials)
    println("VaR : " + valueAtRisk)
    println("ES : " + ExpectedShortFall)
    val csvResults = j map { case (key, value) => Array(key, value).mkString(",\t") }
    val idTostockNameMap = readIdtoStockNameMap(new File(filePrefix))
    val varConfidenceInterval = confidenceInterval(trials, PercentVaR, 100, .05)
    val esConfidenceInterval = confidenceInterval(trials, PercentES, 100, .05)
    println("VaR confidence interval: " + varConfidenceInterval)
    println("ES confidence interval: " + esConfidenceInterval)
    println("Kupiec test p-value: " + BackTestPValue(labels.toSeq, valueAtRisk, 0.05))
  }
}
