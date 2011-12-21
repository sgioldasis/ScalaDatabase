/*
 * Expressions.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package expressions

import java.math.MathContext;
import campaigns._

sealed abstract class Expr {
    def eval(u:String):BigDecimal
}

case class EConst(value:BigDecimal) extends Expr {
    def eval(u:String):BigDecimal = value
}

case class EVar(codKpi:String, valType:String) extends Expr {
    def eval(u:String):BigDecimal = {
        Ref(codKpi, valType).value(u)
    }
}

case class EAdd(left:Expr, right:Expr) extends Expr {
    def eval(u:String):BigDecimal = try {
        left.eval(u) + right.eval(u)
    }
    catch {
        case ex: Exception => null
    }
}

case class ESub(left:Expr, right:Expr) extends Expr {
    def eval(u:String):BigDecimal = try {
        left.eval(u) - right.eval(u)
    }
    catch {
        case ex: Exception => null
    }
}

case class EMul(left:Expr, right:Expr) extends Expr {
    def eval(u:String):BigDecimal = try {
        left.eval(u) * right.eval(u)
    }
    catch {
        case ex: Exception => null
    }
}

case class EDiv(left:Expr, right:Expr) extends Expr {
    def eval(u:String):BigDecimal = {
        try {
            val bd = left.eval(u).bigDecimal.divide(right.eval(u).bigDecimal,MathContext.DECIMAL128)
            //        val bd = left.eval.bigDecimal.divide(right.eval.bigDecimal,BigDecimal.RoundingMode.ROUND_HALF_UP.id)
            val result:BigDecimal = new BigDecimal(bd)
            result
        } catch {
            case ex: Exception => null 
        }
        
    }
}

case class EUMinus(e:Expr) extends Expr {
    def eval(u:String):BigDecimal = try {
        -e.eval(u)
    }
    catch {
        case ex: Exception => null
    }
}

import scala.util.parsing.combinator.lexical.StdLexical

class ExprLexical extends StdLexical {
    override def token: Parser[Token] = floatingToken | super.token

    def floatingToken: Parser[Token] =
    rep1(digit) ~ optFraction ~ optExponent ^^
    { case intPart ~ frac ~ exp => NumericLit(
                (intPart mkString "") :: frac :: exp :: Nil mkString "")}

    def chr(c:Char) = elem("", ch => ch==c )
    def sign = chr('+') | chr('-')
    def optSign = opt(sign) ^^ {
        case None => ""
        case Some(sign) => sign
    }
    def fraction = '.' ~ rep(digit) ^^ {
        case dot ~ ff => dot :: (ff mkString "") :: Nil mkString ""
    }
    def optFraction = opt(fraction) ^^ {
        case None => ""
        case Some(fraction) => fraction
    }
    def exponent = (chr('e') | chr('E')) ~ optSign ~ rep1(digit) ^^ {
        case e ~ optSign ~ exp => e :: optSign :: (exp mkString "") :: Nil mkString ""
    }
    def optExponent = opt(exponent) ^^ {
        case None => ""
        case Some(exponent) => exponent
    }
}

import scala.util.parsing.combinator.syntactical._

object ExprParser extends StandardTokenParsers {
    override val lexical = new ExprLexical
    lexical.delimiters ++= List("+","-","*","/","(",")","_")

    def value = numericLit ^^ { s => EConst(BigDecimal(s)) }

    def variable = ident~"_"~ident ^^ { case s1~"_"~s2 => EVar(s1,s2) } |
                   ident ^^ { s => EVar(s,"ACT") }

    def parens:Parser[Expr] = "(" ~> expr <~ ")"

    def unaryMinus:Parser[EUMinus] = "-" ~> term ^^ { EUMinus(_) }

    def term = ( value |  parens | unaryMinus | variable)

    def binaryOp(level:Int):Parser[((Expr,Expr)=>Expr)] = {
        level match {
            case 1 =>
                "+" ^^^ { (a:Expr, b:Expr) => EAdd(a,b) } |
                "-" ^^^ { (a:Expr, b:Expr) => ESub(a,b) }
            case 2 =>
                "*" ^^^ { (a:Expr, b:Expr) => EMul(a,b) } |
                "/" ^^^ { (a:Expr, b:Expr) => EDiv(a,b) }
            case _ => throw new RuntimeException("bad precedence level "+level)
        }
    }
    val minPrec = 1
    val maxPrec = 2

    def binary(level:Int):Parser[Expr] =
    if (level>maxPrec) term
    else binary(level+1) * binaryOp(level)

    def expr = ( binary(minPrec) | term )

    def parse(s:String) = {
        val tokens = new lexical.Scanner(s)
        phrase(expr)(tokens)
    }

    def apply(s:String):Expr = {
        parse(s) match {
            case Success(tree, _) => tree
            case e: NoSuccess =>
                throw new IllegalArgumentException("Bad syntax: "+s)
        }
    }

//    def test(exprstr: String) = {
//        parse(exprstr) match {
//            case Success(tree, _) =>
//                println("Tree: "+tree)
//                val v = tree.eval()
//                println("Eval: "+v)
//            case e: NoSuccess => Console.err.println(e)
//        }
//    }


}