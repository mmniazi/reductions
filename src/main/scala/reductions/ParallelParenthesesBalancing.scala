package reductions

import common._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig: MeasureBuilder[Unit, Double] = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer new Warmer.Default

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    var parenCount = 0
    for (i <- chars.indices) {
      val char = chars(i)
      if (char == '(') parenCount += 1
      else if (char == ')') parenCount -= 1
      if (parenCount == -1) return false
    }
    parenCount == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(start: Int, end: Int): (Int, Int) = {
      var parenCount = 0
      var negativeParensCount = 0
      for (i <- start to end) {
        val char = chars(i)
        if (char == '(') parenCount += 1
        else if (char == ')') parenCount -= 1
        if (parenCount < negativeParensCount) negativeParensCount = parenCount
      }
      (parenCount, negativeParensCount)
    }

    def reduce(start: Int, end: Int): Boolean = {
      val range = start until end by threshold
      val tuples = range.zip(range.tail)
        .map(t => task({
          traverse(t._1, t._2)
        }))
        .scanLeft((0, true))((t1, t2) => {
          (t1._1 + t2.join()._1, t1._1 + t2.join()._2 >= 0)
        }).toList

      tuples.last._1 == 0 && tuples.map(_._2).reduce(_ && _)
    }

    reduce(0, chars.length)
  }
}
