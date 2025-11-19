package com.scala.config

import com.typesafe.config.Config

class TypeSafeDatabaseSettings(config: Config) extends DatabaseSettings {

  override def url: String = config.getString("url")
  override def user: String = config.getString("user")
  override def password: String = config.getString("password")
  override def driver: String = config.getString("driver")
}
