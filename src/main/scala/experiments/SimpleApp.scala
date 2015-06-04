package experiments
import org.apache.spark.SparkContext._
import scala.collection.mutable._
import java.util.Properties
import java.io.FileInputStream
import splash.core._

object SimpleApp {
  def main(args: Array[String]) {
    val prop = new Properties()
    prop.load(new FileInputStream(args(0)))
    val OptData = "covtype"
    val NlpData = "nytimes"
    
    val root_path = prop.getProperty("root_path")
    val model = prop.getProperty("model")
    if(model.equals("SGD")){ 
      (new SGD).train(root_path + OptData + ".txt")
    }
    if(model.equals("GD")){
      (new GD).train(root_path + OptData + ".txt") 
    }
    if(model.equals("LDA-Gibbs")){
      (new LDA).train(root_path + "vocab." + NlpData + ".txt", root_path + "docword." + NlpData + ".txt")
    }
    if(model.equals("LDA-SVI")){
      (new SVI).train(root_path + "vocab." + NlpData + ".txt", root_path + "docword." + NlpData + ".txt")
    }
    if(model.equals("LDA-VI")){
      (new VI).train(root_path + "vocab." + NlpData + ".txt", root_path + "docword." + NlpData + ".txt")
    }
    if(model.equals("AdaBPR")){
      (new AdaBPR).train(root_path + "netflix.txt") 
    }
    if(model.equals("ALM")){
      (new ALM).train(root_path + "netflix.txt") 
    }
    if(model.equals("Test-Linear-SGD")){ 
      (new TestLinearSGD).train(root_path + OptData + ".txt")
    }
    if(model.equals("Test-LDA")){ 
      (new TestLDA).train(root_path + "docword." + NlpData + ".txt")
    }
  }
  
  val print_values = (sharedVar : SharedVariableSet ) => {
    println("========== Shared Variables ==========")
    for( kv_pair <- sharedVar.variable ){
      println( kv_pair._1 + "=" + kv_pair._2 )
    }
    println("======================================")
  }
}



