package com.scala.service

trait CSVReaderAbstract{

  def readLines(path: String): Iterator[String]
}
