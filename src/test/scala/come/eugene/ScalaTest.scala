package come.eugene

import com.eugene.ScalaJob
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class ScalaTest() {
  var sc: SparkContext = _

  @Before
  def initialize() {
    System.setProperty("hadoop.home.dir", "C:\\WorkSpace\\")
    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
    sc = new SparkContext(conf)
  }

  @After
  def tearDown() {
    sc.stop()
  }

  @Test
  def testExampleJobCode() {
    val input = "src\\test\\resources\\cdr.csv"
    val output = "src\\test\\resources\\out"
    val job = new ScalaJob(sc)
    val result = job.run(input)
    result.saveAsTextFile(output)
  }
}
