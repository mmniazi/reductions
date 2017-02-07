package reductions

import java.util.concurrent._

import scala.collection._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common._
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class LineOfSightSuite extends FunSuite {

	import LineOfSight._

	test("lineOfSight should correctly handle an array of size 4") {
		val output = new Array[Float](4)
		lineOfSight(Array[Float](0f, 1f, 8f, 9f), output)
		assert(output.toList == List(0f, 1f, 4f, 4f))
	}


	test("upsweepSequential should correctly handle the chunk 1 until 4 of an array of 4 elements") {
		val res = upsweepSequential(Array[Float](0f, 1f, 8f, 9f), 1, 4)
		assert(res == 4f)
	}


	test("downsweepSequential should correctly handle a 4 element array when the starting angle is zero") {
		val output = new Array[Float](4)
		downsweepSequential(Array[Float](0f, 1f, 8f, 9f), output, 0f, 1, 4)
		assert(output.toList == List(0f, 1f, 4f, 4f))
	}

	test("line of sight sequential and parallel should give same results") {
		var count = 0
		val size = 10
		val threshold = 1

		def counter = () => {
			count += 1
			count
		}

		val input = Array.fill(size)(Random.nextFloat * 90 * counter())
		input(0) = 0
		var seqOutput = new Array[Float](size)
		val seq = lineOfSight(input, seqOutput)
		val parOutput = new Array[Float](size)
		val par = parLineOfSight(input, parOutput, threshold)
		assert(seqOutput sameElements parOutput)
	}

}

