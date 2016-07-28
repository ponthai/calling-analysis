package org.util

import org.apache.spark.{SparkContext, SparkConf}
import java.net.URLDecoder;
import java.io.UnsupportedEncodingException;
import java.util.Calendar
import com.typesafe.config._
import java.nio.file.Paths
import java.io._

object SparkApplication{
	val CHECKPOINT_DIR: String = "tmpRDDCheckPoint/"
	var sparkContext: SparkContext = null
	var currentTime: Long = 0
	var conf: Config = null

	if(sparkContext == null){
		initializeSpark()
	}

	def initializeSpark(){
		var sparkConf = new SparkConf()
			.setAppName("SparkGraphProcessing")

		sparkContext = new SparkContext(sparkConf)
		val currentPath = Paths.get(".").toAbsolutePath().normalize().toString()
		var pathJar: String = this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath()
		var deCodePath: String = ""
		try{
            deCodePath = URLDecoder.decode(pathJar, "UTF-8")
        }catch {
            case e: UnsupportedEncodingException => e.printStackTrace()
        }
        sparkContext.addJar(deCodePath)

        conf = ConfigFactory.parseFile(new File(currentPath+"/application.conf"))
        println("!!   Config file : " + currentPath + "/application.conf")
        if(conf == null){
        	println("No configuration file (application.conf)")
        	sparkContext.setCheckpointDir(CHECKPOINT_DIR)
        }else{
        	sparkContext.setCheckpointDir(conf.getString("processing.parameters.temporarypath"))
        }
        
	}

	def startApp(args: Array[String]){
		println(sparkContext)

		//Required
		val mode = args(0)
		val algorithm = args(1)
		val processname = args(2)+"_"+algorithm+".txt"
		val input = args(3)
		val separate = args(4)
		var output = conf.getString("processing.parameters.rootouputpath")+"/"+processname
		var numPartition: Int = conf.getString("processing.parameters.defaultgraphpartition").toInt
		//Options
		if(args.length > 5){
			args.length match{
				case 6 => output = args(5)+"/"+processname
				case 7 => numPartition = args(6).toInt
			}
		}

		currentTime = System.currentTimeMillis()
		println("[Status] > TimeStamp = " + currentTime)
		mode match{
			case "FILE" => ProcessManager.runFileData(sparkContext, numPartition, input, algorithm, output, separate)
			case "HIVE" => ProcessManager.runHiveData(sparkContext, numPartition, input, algorithm, output, separate)
		}
		
	}

	def main(args: Array[String]) {
		if(args.length <= 5){
			println("=> Required args should be 5 args :")
			println("args(0) Mode; {HIVE,FILE}")
			println("args(1) Algorithm; {CC,KBC,LC,ALL}")
			println("args(2) ProcessName; Ex. Process_01")
			println("args(3) Input (FullPath); Ex. /user/hddwh01/test/input_*.txt")
			println("args(4) Separator; Ex. - ")
			println("=> Optional args are 2 args : ")
			println("args(5) Output (PathToOutput); Ex. /user/hddwh01/OUTPUT/")
			println("args(6) GraphPartition; Ex. 2")
		}else{
			println("[Status] > Starting ... ")
			startApp(args)
			println("[Status] > Done")
			println("[Status] > Processing Time = " + (System.currentTimeMillis() - currentTime)/ 1000 )	
		}
	}
}