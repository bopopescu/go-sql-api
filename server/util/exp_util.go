package util
import (
	"fmt"
	"github.com/xcltapestry/xclpkg/algorithm"
	"strconv"
	"errors"
	"strings"
	"regexp"
)

//func main(){
//
//	// 中序表达式       后序表达式
//	// a+b            = ab+
//	// (a+b)/c        = ab+c/
//	// a+b*(c+d)      = abcd+*+
//	// a*b+c*(d-e)/f  = ab*cde-*f/+
//
//	//str := "a*b+c*(d-e)/f"
//	//str := "1*2+3*(4-5)/6"
//
//	//str := "1*2+3*(5-1)/2"
//	str := "123+23-45"
//	exp,err := ExpConvert(str)
//	if err != nil {
//		fmt.Println("中序表达式转后序表达式失败! ",err)
//	}else{
//		Exp(exp)
//	}

	//v := 1*2+3*(4-5)/6
//	v := 1*2+3*(5-1)/2
//	fmt.Println("标准结果: ",v)
//
//}
func Calculate(str string)(float64,error){
	var result float64

	//str="-119.12+0"
	expStr := regexp.MustCompile("^([\\-]?[\\d]+[\\.]?[\\d]{0,})([\\-|\\+|\\*|\\/])([\\d]+[\\.]?[\\d]{0,})")
	expArr := expStr.FindStringSubmatch(str)
    for{
    	if len(expArr)>0{
		  exp,error:=	ExpConvert(expArr)
		  if error!=nil{
		  	fmt.Printf("error=",error)
		  }else{
			 result= Exp(exp)
			 resultStr:=strconv.FormatFloat(result, 'f', -1, 64)
			 str=strings.Replace(str,expArr[0],resultStr,-1)
			 expArr = expStr.FindStringSubmatch(str)
		  }
		}else{
			resultF,error:=strconv.ParseFloat(str, 64)
			if error!=nil{
				fmt.Printf("error=",error)
			}else{
				result=resultF
			}
			break
		}
	}
	return result,nil
}

func ExpConvert(strArr []string)(string,error){

	var result string
	stack := algorithm.NewStack()
	for index,s := range strArr {
		fmt.Printf("s=",s)
		if (index+1)==len(strArr){
			break
		}
		ch := string(strArr[index+1])
		if IsOperator(ch) { //是运算符

			if stack.Empty() || ch == "(" {
				stack.Push(ch)
			}else{
				if ch == ")" { //处理括号
					for{
						if stack.Empty() {
							return "",errors.New("表达式有问题! 没有找到对应的\"(\"号")
						}
						if stack.Top().(string) == "(" {
							break
						}
						result += stack.Top().(string)
						stack.Pop()
					}

					//弹出"("
					stack.Top()
					stack.Pop()
				}else{ //非括号
					//比较优先级
					for{
						if stack.Empty() {
							break
						}
						m := stack.Top().(string)
						if Priority(ch) > Priority(m) {
							break
						}
						result += m
						stack.Pop()
					}
					stack.Push(ch)
				}
			}

		}else{	//非运算符
			result += ch+"|"
		} //end IsOperator()

	} //end for range str

	for {
		if stack.Empty() {
			break
		}
		result += stack.Top().(string)
		stack.Pop()
	}

//	fmt.Println("ExpConvert() str    = ",str )
	fmt.Println("ExpConvert() result = ",result)
	return result,nil
}

func Exp(str string)(float64){
	fmt.Println("\nCalc \nExp() :",str )
	var result float64
	stack := algorithm.NewStack()
	arr:=strings.Split(str,"|")
	for _,s := range arr {
		ch := string(s)
		if IsOperator(ch) { //是运算符
			if stack.Empty() {
				break
			}
			b := stack.Top().(string)
			stack.Pop()

			a := stack.Top().(string)
			stack.Pop()

			ia,ib := convToFloat64(a,b)
			sv := fmt.Sprintf("%d",Calc(ch,ia,ib))
			stack.Push( sv )
			fmt.Println("Exp() ",a,"",ch,"",b,"=",sv)
		}else{
			stack.Push(ch)
			fmt.Println("Exp() ch: ",ch)
		} //end IsOperator
	}

	//stack.Print()
	if !stack.Empty() {
		resultV:=stack.Top()
		fmt.Println("表达式运算结果: ", resultV )
		if resultV!=nil{
			resultVstr:=resultV.(string)
			resultVstr=strings.Replace(resultVstr,"%!d(float64=","",-1)
			resultVstr=strings.Replace(resultVstr,")","",-1)
			resultF,error:=strconv.ParseFloat(resultVstr, 64)
			result=resultF
			fmt.Printf("resultF=",resultF)
			if error!=nil{
				fmt.Printf("error=",error)
			}
		}
		stack.Pop()

	}
	return result
}

func convToInt32(a,b string)(int32,int32){

	ia, erra := strconv.ParseInt(a, 10, 32)
	if erra != nil {
		panic(erra)
	}

	ib, errb := strconv.ParseInt(b, 10, 32)
	if errb != nil {
		panic(errb)
	}
	return int32(ia),int32(ib)
}
func convToFloat64(a,b string)(float64,float64){

	ia, erra := strconv.ParseFloat(a, 64)
	if erra != nil {
		panic(erra)
	}

	ib, errb := strconv.ParseFloat(b, 64)
	if errb != nil {
		panic(errb)
	}
	return float64(ia),float64(ib)
}
func IsOperator(op string)(bool){
	switch(op){
	case "(",")","+","-","*","/":
		return true
	default:
		return false
	}
}

func Priority(op string)(int){
	switch(op){
	case "*","/":
		return 3
	case "+","-":
		return 2
	case "(":
		return 1
	default:
		return 0
	}
}

func Calc(op string,a,b float64)(float64){

	switch(op){
	case "*":
		s := fmt.Sprintf("%0.2f", a * b)
		f, _ := strconv.ParseFloat(s, 64)
		return f
	case "/":
		s := fmt.Sprintf("%0.2f", a / b)
		f, _ := strconv.ParseFloat(s, 64)
		return f
	case "+":
		s := fmt.Sprintf("%0.2f", a + b)
		f, _ := strconv.ParseFloat(s, 64)
		return f
	case "-":
		s := fmt.Sprintf("%0.2f", a - b)
		f, _ := strconv.ParseFloat(s, 64)
		return f
	default:
		return 0
	}
}
func ObtainQuarter(month string)(int){

	switch(month){
	case "1","2","3":
		return 1
	case "4","5","6":
		return 2
	case "7","8","9":
		return 3
	case "10","11","12":
		return 4
	default:
		return 0
	}
}