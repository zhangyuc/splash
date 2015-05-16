import org.apache.spark.SparkContext._
import scala.collection.mutable._
import java.util.Properties
import java.io.FileInputStream
import splash._

object SimpleApp {
  def main(args: Array[String]) {
    val prop = new Properties()
    prop.load(new FileInputStream(args(0)))
    
    val root_path = prop.getProperty("root_path")
    val model = prop.getProperty("model")
    if(model.equals("SGD")){ 
      (new SGD).train(root_path + "rcv1.txt")
    }
    if(model.equals("GD")){
      (new GD).train(root_path + "covtype.txt") 
    }
    if(model.equals("LDA-Gibbs")){
      (new LDA).train(root_path + "vocab.nytimes.txt", root_path + "docword.nytimes.txt")
    }
    if(model.equals("LDA-SVI")){
      (new SVI).train(root_path + "vocab.nips.txt", root_path + "docword.nips.txt")
    }
    if(model.equals("LDA-VI")){
      (new VI).train(root_path + "vocab.nips.txt", root_path + "docword.nips.txt")
    }
    if(model.equals("BPR")){
      (new BPR).train(root_path + "netflix.txt") 
    }
    if(model.equals("AdaBPR")){
      (new AdaBPR).train(root_path + "netflix.txt") 
    }
    if(model.equals("ALM")){
      (new ALM).train(root_path + "netflix.txt") 
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



