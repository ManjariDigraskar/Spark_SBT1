package Company.Synechron

object HigherOrderFunc {
  def main(args:Array[String]):Unit={
    val x=4
    val y=5
    val z=6
    funa(x,funb(y,z))
  }
  def funa(a:Int,sum:Int):Unit={

    val total=a+sum
    System.out.println(total)
  }
def funb(b:Int,c:Int):Int={
   b+c
}

}
