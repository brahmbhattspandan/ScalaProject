import java.io.File

import com.github.nscala_time.time.Imports._
import org.scalatest.{FlatSpec,Matchers}
import scala.collection.mutable.{ArrayBuffer, Stack}

/**
  * Created by smit on 11/30/15.
  */
class SetSpec extends FlatSpec with Matchers {

  //------------------------------------------------------------------------------------------
  "A value" should "return its Integer type if the data is integer" in {
    val num = "456"
    val intValue = SimpleSpark.toInt(num)
    intValue shouldBe Some(456)
  }
  it should "return None if the data is of type String" in {
    val num = "ScalaProject"
    val returnValue = SimpleSpark.toInt(num)
    returnValue shouldBe None
  }

  //-------------------------------------------------------------------------------------------
  "Reading Stock Names File" should "File exist and consist of data" in {
    val filePrefix = "/home/smit/Documents/ScalaProject/FilterData/MapData/StockNamesMap.csv"
    val stockNameMap = SimpleSpark.readStockNameMap(new File(filePrefix))
    val size = stockNameMap.size
    size should be > 0
  }

  //-------------------------------------------------------------------------------------------
  "A Date record" should "Fall under the give conditions" in {
    val start = new DateTime(2000, 2, 13, 0, 0)
    val end = new DateTime(2015, 8, 7, 0, 0)

    val sampleDate1 = new DateTime(2012, 5, 25, 12, 12)
    //val sampleDate2 = new DateTime(2011, 6, 14, 0, 0)

    val filled = new ArrayBuffer[(String, DateTime, Double)]()
    filled += (("Val1", sampleDate1, 50.63))
    //filled += (("Val2", sampleDate2, 60.36))
    filled.toArray

    //val returned = new ArrayBuffer[(String, DateTime, Double)]()
    val ret = SimpleSpark.trimToRegion(filled.toArray, start, end)

    assert (ret.size === 3)
  }

  //-----------------------------------------------------------------------------------------

}