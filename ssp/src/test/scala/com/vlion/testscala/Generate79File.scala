package com.vlion.testscala

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

/**
 * @description:
 * @author: malichun
 * @time: 2021/1/12/0012 16:35
 *
 */
object Generate79File {
    def main(args: Array[String]): Unit = {
        val originFilePath = args(0)
        val realFilePath = args(1)
        val predict_file_path = args(2)

        val originFile = Source.fromFile(originFilePath)
        //真实的
        val originLines = originFile.getLines()
        val filteredLines = originLines.map(_.split("#"))
            .filter(_ (2) == "79").toList

        //真实的0,1
        writeToFile(filteredLines,realFilePath, arr => {
            arr(1)
        })

        //被预测的文件,每行是个json数组
        writeToFile(filteredLines,predict_file_path,arr => {

            val buffer = arr.toBuffer
            println(buffer)
            buffer.remove(0,2)
            "["+buffer.mkString(",")+"]"
        })

        val arr = Array[Int]()
        arr map { i => i+2 }



    }

    def writeToFile(elements: List[Array[String]], fileName: String, process: Array[String] => String): Unit = {
        val bufferedWriter = new BufferedWriter(new FileWriter(new File(fileName)))
        elements.foreach( arr => {
            bufferedWriter.write( process(arr) )
            bufferedWriter.newLine()
            bufferedWriter.flush()
        })

        bufferedWriter.close()
    }

}
