import java.util.concurrent.ConcurrentHashMap


object Test extends App {

//  val str = "http://n.e24c.com/tage-api-user-notify/binding-notify"
//  val threadNameSplits = str.split("-").drop(2).dropRight(1).fold("") { (x, y) => x + "-" + y }.drop(1)
//  val clientAndProfile = threadNameSplits.split(":")
//  val tup = (clientAndProfile(0), clientAndProfile.drop(1).fold("") { (x, y) => x + y })

//  println(tup._1)
//  println(tup._2)
//  println(str.startsWith("http://n.e24c.com"))
  
   val _synchronizedMap = new ConcurrentHashMap[String, String]
   _synchronizedMap.put("hello","1")
   val adderSupplier = new java.util.function.BiFunction[String, String, String]() {
      override def apply(t: String, u: String): String = {
        if (u == null)
          "test"
        else
          "1"+"test"

      }
    }
    _synchronizedMap.compute("hello", adderSupplier)
    
    println(_synchronizedMap.get("hello"))
   
}