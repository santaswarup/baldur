import scopt.OptionParser


object BaldurConfig {

  case class Config(in: java.net.URI=new java.net.URI("in"),
                    out: java.net.URI=new java.net.URI("out"),
                    client: String="",
                    inputType: String="",
                    brokerList: String="localhost:9092",
                    interval: Int=30)

  def getConfig(args: Array[String]) : Config = {
    //Define options
    val optionParser = new OptionParser[Config]("Baldur") {
      opt[java.net.URI]('i', "in") required() valueName "<input_directory>" action { (x, c) =>
        c.copy(in = x)
      }

      opt[java.net.URI]('o', "out") required() valueName "<output_directory>" action { (x, c) =>
        c.copy(out = x)
      }

      opt[String]('c', "client") required() valueName "<client_key>" action { (x, c) =>
        c.copy(client = x)
      }

      opt[String]("type") required() valueName "<input_type>" action { (x, c) =>
        c.copy(inputType = x)
      }

      opt[String]("metadata.broker.list") valueName "<server:port>" action { (x, c) =>
        c.copy(brokerList = x)
      }

      opt[Int]('i', "interval") valueName "<spark_streaming_batch_interval_seconds>" action { (x, c) =>
        c.copy(interval = x)
      }
    }

    optionParser.parse(args, Config()) match {
      case Some(config) => return config
      case None => sys.exit(1)
    }


  }
}