//https://raw.githubusercontent.com/sramirez/spark-RELIEFFC-fselection/3ce4b6cc7f3dcee275b0c72fc6b443f189b773d3/src/main/scala/org/apache/spark/ml/util/MatrixAccumulator.scala
package org.apache.spark.ml.util


import breeze.linalg._
import breeze.numerics._
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.ArrayBuffer

/**
 * @author sramirez
 */
class MatrixAccumulator(val rows: Int, val cols: Int, sparse: Boolean) extends AccumulatorV2[Matrix[Double], Matrix[Double]] {
  
  def this(m: Matrix[Double]) = {
    this(m.rows, m.cols, m.isInstanceOf[CSCMatrix[Double]])
    this.accMatrix = m.copy
  }
  
  def this(rows: Int, cols: Int, cooMatrix: ArrayBuffer[(Int, Int, Double)]) = {
    this(rows, cols, true)
    val builder = new CSCMatrix.Builder[Double](rows = rows, cols = cols)
    cooMatrix.foreach{ t => builder.add(t._1, t._2, t._3) }
    this.accMatrix = builder.result
  }

  private var accMatrix: Matrix[Double] = if (sparse) CSCMatrix.zeros(rows, cols) else Matrix.zeros(rows, cols) 
  private var zero: Boolean = true

  def reset(): Unit = {
    accMatrix = if (sparse) CSCMatrix.zeros(rows, cols) else Matrix.zeros(rows, cols) 
    zero = true
  }

  def add(v: Matrix[Double]): Unit = {
    if(isZero) 
      zero = false
    accMatrix = accMatrix match {
      case bsm: CSCMatrix[Double] => bsm += v.asInstanceOf[CSCMatrix[Double]]
      case dsm: DenseMatrix[Double] => dsm += v
    }
  }
  
  def isZero(): Boolean = zero
  
  def merge(other: AccumulatorV2[Matrix[Double], Matrix[Double]]): Unit = add(other.value)
  
  def value: Matrix[Double] = accMatrix
  
  def copy(): AccumulatorV2[Matrix[Double], Matrix[Double]] = new MatrixAccumulator(accMatrix)
}