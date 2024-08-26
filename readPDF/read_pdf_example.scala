// spark-shell -i /home/hduser/projects/scala/workspace/sparkshellscripts/read_pdf_example.scala  --driver-memory 5g --jars /home/hduser/projects/java/jar/pdfbox-app-3.0.1.jar --driver-class-path /home/hduser/projects/java/jar/pdfbox-app-3.0.1.jar

import java.io.File
import org.apache.pdfbox.Loader
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.fontbox.cmap.CMapParser
val pdfPath = "/home/hduser/projects/scala/data/abnamro/pdf/Rekeningafschrift_Volgnr20180005.pdf"  // Replace with the actual path
val document = Loader.loadPDF(new File(pdfPath))
val stripper = new PDFTextStripper()
stripper.setSortByPosition(true) // Set to preserve the original text structure
val pdfText = stripper.getText(document)
println(pdfText)
document.close()


//System.exit(0)

// Distributed =>

	//import org.apache.spark.SparkContext
	//import org.apache.spark.SparkConf
	//import org.apache.pdfbox.pdmodel.PDDocument
	//import org.apache.pdfbox.text.PDFTextStripper
	//
	//object PDFToText {
	//  def main(args: Array[String]): Unit = {
	//    val conf = new SparkConf().setAppName("PDF to Text Conversion")
	//    val sc = new SparkContext(conf)
	//    
	//    val pdfFiles = sc.binaryFiles("hdfs://path/to/pdf/files/*.pdf") // Replace with your PDF files location
	//    
	//    val text = pdfFiles.flatMap { case (_, file) =>
	//      val pdf = PDDocument.load(file.open())
	//      val stripper = new PDFTextStripper()
	//      val text = stripper.getText(pdf)
	//      pdf.close()
	//      text.split("\n")
	//    }
	//    
	//    text.saveAsTextFile("hdfs://path/to/output") // Replace with your output location
	//    
	//    sc.stop()
	//  }
	//}
