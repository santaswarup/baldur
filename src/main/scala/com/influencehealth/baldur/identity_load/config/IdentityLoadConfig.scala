package com.influencehealth.baldur.identity_load.config

import java.net.URI

import scopt.OptionParser

object IdentityLoadConfig {

  case class Config(
    in: URI=new URI("in"),
    brokerList: String="localhost:9092",
    inputSource: String="",
    outputTopic: String="identity_input",
    delimiter: Option[String]=None)

  def getConfig(args: Array[String]) : Config = {
    //Define options
    val optionParser = new OptionParser[Config]("Baldur") {
      opt[java.net.URI]('i', "in") required() valueName "<input_directory>" action { (x, c) =>
        c.copy(in = x)
      }

      opt[String]("metadata.broker.list") valueName "<server:port>" action { (x, c) =>
        c.copy(brokerList = x)
      }

      opt[String]("inputSource") required() valueName "<inputSource>" action { (x, c) =>
        c.copy(inputSource = x)
      }

      opt[String]("outputTopic") valueName "<outputTopic>" action { (x, c) =>
        c.copy(outputTopic = x)
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