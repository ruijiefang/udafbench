class CityRemarkUDAF extends Aggregator[String, Buffer, String] {

    // 缓冲区初始化
    override def zero: Buffer = {
      Buffer(0L, mutable.Map[String, Long]())
    }

    // 更新缓冲区数据
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total += 1
      val newCnt = buff.cityMap.getOrElse(city, 0L) + 1
      buff.cityMap.update(city, newCnt)
      buff
    }

    // 合并缓冲区
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total

      val map1 = b1.cityMap
      val map2 = b2.cityMap

      map2.foreach {
        case (city, cnt) => {
          val newCnt = map1.getOrElse(city, 0L) + cnt
          map1.update(city, newCnt)
        }
      }
      b1.cityMap = map1
      b1
    }

  }
