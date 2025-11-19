package com.scala.config

trait ConfigLoader {

  def csvPath: String
  def batchSize: Int
}
