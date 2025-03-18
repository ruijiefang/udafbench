//https://raw.githubusercontent.com/lruns/big-data-processing/ba23f7dba012c7d1d0a0760ea14c196d3ddd5b37/Homework2/src/main/scala/org/apache/spark/ml/made/LinearRegression.scala
package org.apache.spark.ml.made

import breeze.linalg.{norm, sum, Vector => BV, axpy => brzAxpy}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

trait LinearRegressionParams extends HasInputCol with HasOutputCol {

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(inputCol, "features")
  setDefault(outputCol, "label")

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, getInputCol, new VectorUDT())

    if (schema.fieldNames.contains($(outputCol))) {
      SchemaUtils.checkNumericType(schema, getOutputCol)
      schema
    } else {
      SchemaUtils.appendColumn(schema, schema(getInputCol).copy(name = getOutputCol))
    }
  }
}

class LinearRegression(override val uid: String,
                       val stepSize: Double,
                       val maxIter: Int)
  extends Estimator[LinearRegressionModel]
    with LinearRegressionParams
    with DefaultParamsWritable
    with MLWritable {

  def this() = this(Identifiable.randomUID("linearRegression"), 0.001, 10000)

  def this(uid: String) = this(uid, 0.001, 10000)

  def this(stepSize: Double, maxIter: Int) = this(
    Identifiable.randomUID("linearRegression"),
    stepSize,
    maxIter)

  private val gradient = new LeastSquaresGradient()
  private val updater = new SimpleUpdater()
  private val optimizer = new GradientDescent(gradient, updater, $(inputCol), $(outputCol))
    .setStepSize(stepSize)
    .setNumIterations(maxIter)

  override def setInputCol(value: String): this.type = {
    set(inputCol, value)
    optimizer.setInputCol($(inputCol))
    this
  }

  override def setOutputCol(value: String): this.type = {
    set(outputCol, value)
    optimizer.setOutputCol($(outputCol))
    this
  }

  override def fit(dataset: Dataset[_]): LinearRegressionModel = {
    var weights: Vector = Vectors.dense(1.0, 1.0, 1.0, 1.0)

    val withOnes = dataset.withColumn("ones", functions.lit(1))
    val assembler = new VectorAssembler()
      .setInputCols(Array($(inputCol), "ones"))
      .setOutputCol("features_extended")

    val assembled = assembler
      .transform(withOnes)
      .select(functions.col("features_extended").as($(inputCol)), functions.col($(outputCol)))

    weights = optimizer.optimize(assembled, weights)

    copyValues(new LinearRegressionModel(new DenseVector(
      weights.toArray.slice(0, weights.size - 1)),
      weights.toArray(weights.size - 1)))
      .setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[LinearRegressionModel] = {
    copyValues(new LinearRegression(stepSize, maxIter))
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def write: MLWriter = new LinearRegression.LinearRegressionWriter(this)
}

object LinearRegression extends DefaultParamsReadable[LinearRegression] with MLReadable[LinearRegression] {
  override def read: MLReader[LinearRegression] = new LinearRegressionReader

  override def load(path: String): LinearRegression = super.load(path)

  private class LinearRegressionWriter(instance: LinearRegression) extends MLWriter {

    private case class Data(stepSize: Double, numIterations: Int)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)

      val stepSize = instance.stepSize
      val numIterations = instance.maxIter
      val data = Data(stepSize, numIterations)
      val dataPath = new Path(path, "data").toString
      sparkSession
        .createDataFrame(Seq(data))
        .repartition(1)
        .write
        .parquet(dataPath)
    }
  }

  private class LinearRegressionReader extends MLReader[LinearRegression] {
    private val className = classOf[LinearRegression].getName

    override def load(path: String): LinearRegression = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("stepSize", "numIterations").head()
      val stepSize = data.getAs[Double](0)
      val numIterations = data.getAs[Int](1)

      val transformer = new LinearRegression(metadata.uid, stepSize, numIterations)
      metadata.getAndSetParams(transformer)
      transformer.optimizer.setInputCol(transformer.getInputCol)
      transformer.optimizer.setOutputCol(transformer.getOutputCol)

      transformer
    }
  }
}

class LinearRegressionModel private[made](
                                           override val uid: String,
                                           val weights: DenseVector,
                                           val bias: Double)
  extends Model[LinearRegressionModel]
    with LinearRegressionParams
    with MLWritable {

  private[made] def this(weights: DenseVector, bias: Double) =
    this(Identifiable.randomUID("linearRegressionModel"), weights, bias)

  override def copy(extra: ParamMap): LinearRegressionModel = {
    copyValues(new LinearRegressionModel(weights, bias))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val transformUdf = dataset.sqlContext.udf.register(uid + "_transform",
      (x: Vector) => {
        sum(x.asBreeze *:* weights.asBreeze) + bias
      })

    dataset.withColumn($(outputCol), transformUdf(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def write: MLWriter = new LinearRegressionModel.LinearRegressionModelWriter(this)
}

object LinearRegressionModel extends DefaultParamsReadable[LinearRegressionModel] with MLReadable[LinearRegressionModel] {
  override def read: MLReader[LinearRegressionModel] = new LinearRegressionModelReader

  override def load(path: String): LinearRegressionModel = super.load(path)

  private class LinearRegressionModelWriter(instance: LinearRegressionModel) extends MLWriter {

    private case class Data(weights: DenseVector, bias: Double)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)

      val weights = instance.weights
      val bias = instance.bias
      val data = Data(weights, bias)
      val dataPath = new Path(path, "data").toString
      sparkSession
        .createDataFrame(Seq(data))
        .repartition(1)
        .write
        .parquet(dataPath)
    }
  }

  private class LinearRegressionModelReader extends MLReader[LinearRegressionModel] {

    private val className = classOf[LinearRegressionModel].getName

    override def load(path: String): LinearRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("weights", "bias").head()
      val weights = data.getAs[DenseVector](0)
      val bias = data.getAs[Double](1)

      val model = new LinearRegressionModel(metadata.uid, weights, bias)
      metadata.getAndSetParams(model)

      model
    }
  }
}

abstract class Gradient private[made] extends Serializable {
  def compute(data: Vector, label: Double, weights: Vector): (Vector, Double)
}

class LeastSquaresGradient private[made] extends Gradient {
  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val diff = sum(data.asBreeze *:* weights.asBreeze) - label
    val loss = diff * diff / 2.0
    val gradient = data.copy.asBreeze * diff
    (Vectors.fromBreeze(gradient), loss)
  }
}

trait Optimizer extends Serializable {
  def optimize(dataset: Dataset[_], initialWeights: Vector): Vector
}

class GradientDescent private[made](private var gradient: Gradient,
                                    private var updater: Updater,
                                    private var inputCol: String,
                                    private var outputCol: String)
  extends Optimizer with Logging {

  private var stepSize: Double = 0.001
  private var numIterations: Int = 100
  private var convergenceTol: Double = 0.001

  def setStepSize(step: Double): this.type = {
    require(step > 0,
      s"Initial step size must be positive but got ${step}")
    this.stepSize = step
    this
  }

  def setNumIterations(iters: Int): this.type = {
    require(iters >= 0,
      s"Number of iterations must be nonnegative but got ${iters}")
    this.numIterations = iters
    this
  }

  def setInputCol(inputCol: String): this.type = {
    this.inputCol = inputCol
    this
  }

  def setOutputCol(outputCol: String): this.type = {
    this.outputCol = outputCol
    this
  }

  def optimize(dataset: Dataset[_], initialWeights: Vector): Vector = {
    val (weights, _) = GradientDescent.runGD(
      dataset,
      gradient,
      updater,
      stepSize,
      numIterations,
      initialWeights,
      convergenceTol,
      inputCol,
      outputCol)
    weights
  }
}

object GradientDescent extends Logging {
  def runGD(dataset: Dataset[_],
            gradient: Gradient,
            updater: Updater,
            stepSize: Double,
            numIterations: Int,
            initialWeights: Vector,
            convergenceTol: Double,
            inputCol: String,
            outputCol: String): (Vector, Array[Double]) = {
    val iterationsNotChanged = 5

    val LossHistory = new ArrayBuffer[Double](numIterations)
    var bestLoss = Double.MaxValue
    var badItersCount = 0

    var previousWeights: Option[Vector] = None
    var currentWeights: Option[Vector] = None

    val numExamples = dataset.count()

    if (numExamples == 0) {
      logWarning("GradientDescent.runGD returning initial weights, no data found")
      return (initialWeights, LossHistory.toArray)
    }

    var weights = Vectors.dense(initialWeights.toArray)
    var bestWeights = weights
    val weights_count = weights.size
    val rows_count = dataset.count()

    var converged = false
    var i = 1
    while (!converged && i <= numIterations) {

      val customSummer = new Aggregator[Row, (Vector, Double), (Vector, Double)] {
        def zero: (Vector, Double) = (Vectors.zeros(weights_count), 0.0)

        def reduce(acc: (Vector, Double), x: Row): (Vector, Double) = {
          val (grad, loss) = gradient.compute(x.getAs[Vector](inputCol), x.getAs[Double](outputCol), weights)
          (Vectors.fromBreeze(acc._1.asBreeze + grad.asBreeze / rows_count.asInstanceOf[Double]), acc._2 + loss / rows_count.asInstanceOf[Double])
        }

        def merge(acc1: (Vector, Double), acc2: (Vector, Double)): (Vector, Double) = (Vectors.fromBreeze(acc1._1.asBreeze + acc2._1.asBreeze), acc1._2 + acc2._2)

        def finish(r: (Vector, Double)): (Vector, Double) = r

        override def bufferEncoder: Encoder[(Vector, Double)] = ExpressionEncoder()

        override def outputEncoder: Encoder[(Vector, Double)] = ExpressionEncoder()
      }.toColumn

      val row = dataset.select(customSummer.as[(Vector, Double)](ExpressionEncoder()))

      val loss = row.first()._2
      LossHistory += loss
      weights = updater.compute(weights, row.first()._1, stepSize, i)

      if (loss < bestLoss) {
        bestLoss = row.first()._2
        bestWeights = weights
        badItersCount = 0
      } else {
        badItersCount += 1
      }

      previousWeights = currentWeights
      currentWeights = Some(weights)
      if (previousWeights.isDefined && currentWeights.isDefined) {
        if (convergenceTol == 0.0) {
          converged = badItersCount > iterationsNotChanged
        } else {
          converged = isConverged(previousWeights.get, currentWeights.get, convergenceTol)
        }
      }
      i += 1
    }

    logInfo("GradientDescent.runGD finished. Last 10 losses %s".format(LossHistory.takeRight(10).mkString(", ")))

    (weights, LossHistory.toArray)
  }

  def isConverged(previousWeights: Vector,
                  currentWeights: Vector,
                  convergenceTol: Double): Boolean = {
    val previousBDV = previousWeights.asBreeze.toDenseVector
    val currentBDV = currentWeights.asBreeze.toDenseVector

    val solutionVecDiff: Double = norm(previousBDV - currentBDV)

    solutionVecDiff < convergenceTol * Math.max(norm(currentBDV), 1.0)
  }

}

abstract class Updater private[made] extends Serializable {

  def compute(
               weightsOld: Vector,
               gradient: Vector,
               stepSize: Double,
               iter: Int): Vector
}

class SimpleUpdater private[made] extends Updater {
  override def compute(weightsOld: Vector,
                       gradient: Vector,
                       stepSize: Double,
                       iter: Int): Vector = {
    val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
    brzAxpy(-stepSize, gradient.asBreeze, brzWeights)

    Vectors.fromBreeze(brzWeights)
  }
}
