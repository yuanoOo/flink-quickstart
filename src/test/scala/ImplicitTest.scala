package scala

import scala.language.implicitConversions

object ImplicitTest extends App {

    trait PrintStr {
        val value: String

        def printSeperator(sep: String): Unit = {
            println(value.split("").mkString(sep))
        }
    }

    implicit def str2PrintStr(str: String): PrintStr = new PrintStr {
        override val value: String = str
    }


    "sdsd" printSeperator "#"
}
