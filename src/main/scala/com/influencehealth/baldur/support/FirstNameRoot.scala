package com.influencehealth.baldur.support

import scala.io.Source

case class FirstNameRoot(firstName: String,
   sex: String,
   rootName: String)

object FirstNameRoot{
  
  val rootNames = Source.fromURL(getClass.getResource("/rootnames.csv")).getLines().map(FirstNameRoot.create).toSeq

  def create(line: String): FirstNameRoot = {
    
  	val splitLine = line.split(",")

    FirstNameRoot(
    		firstName = splitLine(0),
    		sex = splitLine(1),
    		rootName = splitLine(2)
    	)
  }

  def getRootName(firstName: Option[String], sex: Option[String]): Option[String] = (firstName, sex) match {
      
      case (None, _) => None
      case (Some(firstName), None) => Some(firstName.toLowerCase)
      case (Some(firstName), Some(sex)) => {
     	   
      	val matched = rootNames.find(record => record.firstName == firstName.toLowerCase && record.sex == sex.toLowerCase)
    	
        matched match{
          case Some(firstNameRoot: FirstNameRoot) => Some(firstNameRoot.rootName.toLowerCase)
          case None => Some(firstName.toLowerCase)
        }
    }
    	
  }

}




