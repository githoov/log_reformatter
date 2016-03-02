// preliminairies
package com.looker.converter
import com.typesafe.config.ConfigFactory

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.regex.Matcher
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object LogReformatter {

 def main(args: Array[String]) {

  // general configuration
  val conf = new SparkConf().setAppName("Log Reformatter")
  val sc = new SparkContext(conf)
  val config = new Settings(ConfigFactory.load())

  // load aws credentials and S3 locations
  val aws_key = config.key
  val aws_secret = config.secret
  val aws_source_bucket = config.source_bucket
  val aws_destination_bucket = config.destination_bucket

  // hadoop configuration for s3
  val hadoopConf = sc.hadoopConfiguration
  hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hadoopConf.set("fs.s3n.awsAccessKeyId", aws_key)
  hadoopConf.set("fs.s3n.awsSecretAccessKey", aws_secret)

  // define input dateformat
  val fromDateFormat = new java.text.SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z")

  // define output dateformat
  val toDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss Z")

  // set time zone to utc
  toDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  // regexp to match stuff we want
  val lineMatch = "([0-9\\.]+)\\s+(?:\\[)(.*)(?:\\])\\s+(\\S+)\\s+(\\S+)\\?(\\S+)\\s+(\\S+)\\s+(\\S+)(.*)".r  

  def cleanLine(rawLine: String): String = {
    rawLine.replace("\"", "")
  }

  def extractValues(line: String): Option[logLine] = {
    line match {
      case lineMatch(ip,dateTime, method, uriStem, uriQuery, http, agent, _*) 
        => return Option(logLine(ip, dateTime, method, uriStem, uriQuery, http, agent))
      case _ 
        =>  None
    }
  }

  def convertDates(line: logLine): logLine = {
    val parsedDate = fromDateFormat.parse(line.dateTime)
    val formattedDate = toDateFormat.format(parsedDate)
    logLine(line.ip, formattedDate, line.method, line.uriStem, line.uriQuery, line.http, line.agent)
  }

  def reformatLog(line: logLine): String = {
    val splitTime = line.dateTime.split(" ")
    val date = splitTime(0)
    val time = splitTime(1)
    val edgeLocation = "IAD53"
    val bytes = 471
    val ip = line.ip
    val method = line.method
    val host = "d2cmtze4pur4i9.cloudfront.net"
    val uriStem = line.uriStem
    val status = 200
    val referrer = "-"
    val agent = line.agent
    val uriQuery = line.uriQuery
    s"$date\t$time\t$edgeLocation\t$bytes\t$ip\t$method\t$host\t$uriStem\t$status\t$referrer\t$agent\t$uriQuery"
  }

  def run(line: String): Option[String] = {
    val cleanedLine = cleanLine(line)
    val parsedLine = extractValues(cleanedLine)

    if (parsedLine.nonEmpty) {
    val convertedLine = convertDates(parsedLine.get)
    val formattedLog = reformatLog(convertedLine)
    return Some(formattedLog)
    }
    None
  }

  // read lines
  val logLines = sc.textFile(aws_source_bucket)

  // convert logs
  val convertedLines = logLines.flatMap(run)

  // save converted logs
  convertedLines.saveAsTextFile(aws_destination_bucket)
 }
}
