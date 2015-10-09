package com.influencehealth.baldur.intake.config

import scopt.OptionParser


object IntakeConfig {

  case class Config(in: java.net.URI=new java.net.URI("in"),
    out: java.net.URI=new java.net.URI("out"),
    client: String="",
    brokerList: String="localhost:9092",
    interval: Int=30,
    source: String="",
    sourceType: String="",
    delimiter: Option[String]=None,
    processDrg: Boolean=false)

  def getConfig(args: Array[String]) : Config = {
    //Define options
    val optionParser = new OptionParser[Config]("Baldur") {
      opt[java.net.URI]('i', "in") required() valueName "<input_directory>" action { (x, c) =>
        c.copy(in = x)
      }

      opt[java.net.URI]('o', "out") required() valueName "<output_directory>" action { (x, c) =>
        c.copy(out = x)
      }

      opt[Boolean]('d', "drg") required() valueName "<process_drg_grouper>" action { (x, c) =>
        c.copy(processDrg = x)
      }

      opt[String]('c', "client") required() valueName "<client_key>" action { (x, c) =>
        c.copy(client = x)
      }

      opt[String]("metadata.broker.list") valueName "<server:port>" action { (x, c) =>
        c.copy(brokerList = x)
      }

      opt[Int]('i', "interval") valueName "<spark_streaming_batch_interval_seconds>" action { (x, c) =>
        c.copy(interval = x)
      }

      opt[String]("source") required() valueName "<source>" action { (x, c) =>
        c.copy(source = x)
      }

      opt[String]("source-type") required() valueName "<source_type>" action { (x, c) =>
        c.copy(sourceType = x)
      }

      opt[String]("delimiter") valueName "<delimiter>" action { (x, c) =>
        if (x.length() > 0) {
          c.copy(delimiter = Some(x))
        } else {
          c.copy(delimiter = None)
        }
      }
    }

    optionParser.parse(args, Config()) match {
      case Some(config) => config
      case None => sys.exit(1)
    }


  }
}
