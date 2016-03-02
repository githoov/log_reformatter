package com.looker.converter

case class logLine (
  ip: String,
  dateTime: String,
  method: String,
  uriStem: String,
  uriQuery: String,
  http: String,
  agent: String
)
