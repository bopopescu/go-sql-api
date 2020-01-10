package util

import (
	"crypto/md5"
	"math/rand"
	"encoding/base64"
	"encoding/hex"
	"github.com/shiyongabc/go-sql-api/server/lib"

	"fmt"
	"github.com/shiyongabc/jwt-go"
	"strconv"
	"strings"
	"time"
)

var (
	machineID     int64 // 机器 id 占10位, 十进制范围是 [ 0, 1023 ]
	sn            int64 // 序列号占 12 位,十进制范围是 [ 0, 4095 ]
	lastTimeStamp int64 // 上次的时间戳(毫秒级), 1秒=1000毫秒, 1毫秒=1000微秒,1微秒=1000纳秒
)


func init() {
	lastTimeStamp = time.Now().UnixNano() / 1000000
}


func SetMachineId(mid int64) {
	// 把机器 id 左移 12 位,让出 12 位空间给序列号使用
	mid=rand.Int63n(10)
	machineID = mid << 12
}

func GetSnowflakeId() int64 {
	curTimeStamp := time.Now().UnixNano() / 1000000
    println("machineID=",machineID)
	// 同一毫秒
	if curTimeStamp == lastTimeStamp {
		sn++
		// 序列号占 12 位,十进制范围是 [ 0, 4095 ]
		if sn > 4095 {
			time.Sleep(time.Millisecond)
			curTimeStamp = time.Now().UnixNano() / 1000000
			lastTimeStamp = curTimeStamp
			sn = 0
		}

		// 取 64 位的二进制数 0000000000 0000000000 0000000000 0001111111111 1111111111 1111111111  1 ( 这里共 41 个 1 )和时间戳进行并操作
		// 并结果( 右数 )第 42 位必然是 0,  低 41 位也就是时间戳的低 41 位
		rightBinValue := curTimeStamp & 0x1FFFFFFFFFF
		// 机器 id 占用10位空间,序列号占用12位空间,所以左移 22 位; 经过上面的并操作,左移后的第 1 位,必然是 0
		rightBinValue <<= 22
		id := rightBinValue | machineID | sn
		return id
	}


	if curTimeStamp > lastTimeStamp {
		sn = 0
		lastTimeStamp = curTimeStamp
		// 取 64 位的二进制数 0000000000 0000000000 0000000000 0001111111111 1111111111 1111111111  1 ( 这里共 41 个 1 )和时间戳进行并操作
		// 并结果( 右数 )第 42 位必然是 0,  低 41 位也就是时间戳的低 41 位
		rightBinValue := curTimeStamp & 0x1FFFFFFFFFF
		// 机器 id 占用10位空间,序列号占用12位空间,所以左移 22 位; 经过上面的并操作,左移后的第 1 位,必然是 0
		rightBinValue <<= 22
		id := rightBinValue | machineID | sn
		return id+rand.Int63n(10)
	}


	if curTimeStamp < lastTimeStamp {
		return 0
	}

	return 0
}
func TypeOf(v interface{}) string {
	return fmt.Sprintf("%T", v)
}
func ObtainUserByToken(authorization string,key string) string{
	if authorization==""{
		return ""
	}
	jwtToken:=  authorization;
	jwtToken= strings.Replace(jwtToken,"bearer%20","",-1)
	token,error:=  jwt.Parse(jwtToken,GetValidationKey)
	lib.Logger.Infof("jwtToken=",error)
	//token,error:=ParseWithClaims(jwtToken,MapClaims{},getValidationKey)
	//  a,error:=  jwt.DecodeSegment(jwtToken)
	var cl jwt.MapClaims
	//	var cc Claims
	cl = token.Claims.(jwt.MapClaims)
	userJwt:=cl[key]
	var userJwtStr string
	switch userJwt.(type){
	case string:
		if userJwt!=nil{
			userJwtStr=userJwt.(string)
		}
	case float64:
		if userJwt!=nil{
			userJwtStr=strconv.FormatFloat(userJwt.(float64), 'f', -1, 64)
		}
	}
	return userJwtStr
}
func GetBetweenStr(str, start, end string) string {
	n := strings.Index(str, start)
	if n == -1 {
		n = 0
	}
	str = string([]byte(str)[n:])
	m := strings.Index(str, end)
	if m == -1 {
		m = len(str)
	}
	str = string([]byte(str)[:m])
	if strings.Contains(str,start){
		return strings.Replace(str,start,"",-1)
	}
	return str
}
func GetValidationKey(*jwt.Token) (interface{}, error) {
	//return []byte("-----BEGIN PUBLIC KEY-----\n"+
	//"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAgNyqMbehSVf5AxAVO+v/K3FmgkvwKeI0VcySCDjl/Lag55EuOxBWUPLKBu/ujnpK34mohr0uhPn/UhawNuXM96zz1wKEFUqE8F9Srwg/V2o+Ugl8ZuCQxtSpCVVwc+RfpL60Y5zWYlrYO2JTmCIhfZ9cG4NzE0n/TV6PHeVsjpucFiMcUD+V6nHDSzuXCOVnp1UIuaf8cL3y1EXDanndYeABeOt2xg3elXLNO5VGJTKfhstbfn/YspdBScA7tGR5uQ4upHD4pIg6OxCyTs27DvnIAQMdQ+OnMJR02e4gC1eDw//txsw/y2UcsZFthfK77lvACPySBukiK+C0qjLBfj9QIDAQAB\n"+
	//"-----END PUBLIC KEY-----"), nil
	//return []byte("MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCA3Koxt6FJV/kDEBU76/8rcWaCS/Ap4jRVzJIIOOX8tqDnkS47EFZQ8soG7+6OekrfiaiGvS6E+f9SFrA25cz3rPPXAoQVSoTwX1KvCD9Xaj5SCXxm4JDG1KkJVXBz5F+kvrRjnNZiWtg7YlOYIiF9n1wbg3MTSf9NXo8d5WyOm5wWIxxQP5XqccNLO5cI5WenVQi5p/xwvfLURcNqed1h4AF463bGDd6Vcs07lUYlMp+Gy1t+f9iyl0FJwDu0ZHm5Di6kcPikiDo7ELJOzbsO+cgBAx1D46cwlHTZ7iALV4PBPGzD/LZRyxkW2F8rvuW8AI/JIG6SIr4LSqMsF+P1AgMBAAECggEANrBwMuWCOAR0FE6xFFtWUnOwU8AyzzPHjlph58duJFDF/UFqY3rNh1FjWIpfrmxMdo6PzY9gvOL07zvd0Y657KukWS4iLH8R6IosJ0jSySC4Dk0kVO0dxKTgkKuILEdSKDMfj98yRU/U0W8rlzd1C0Gk77BcGGWhSo7FIqUJ64OVUvwW1BWkwkZ7aXFtYvgcyN1AmMjc8AoJzJdEoBKlAw41/WMESt6QSKWoWziUxdkrmRlUsvwQHfP9c2BZnkxIPhpGDSkIHAeBj47+JC3hsFuqZM5ahwdma9p9ONz/00PrnB5p399mqW2dknzBIg1TRg2xN9DcqNeo07U0AGJLSQKBgQDiUZIH0rzruLtjF33JQDRc963kfgJHGxM9inH3FJVE7NDJHaOyVjaIxRPG5/7RGEYrrJDZE69iP2ohzK6ew7nGbZN7KaKfKY5Vc4egVZcBMubR5+djwjIqHMTcuJgz/Qu17cvF42rUqpBnS2BbWDRj6+TE0zYEjqdsXri+UiXHgwKBgQCRwxTBEEqazkw8+EtV1XPyp5u3Er/sg3T6CVWU7vsGX8l2bpl3CEUXawkkZaffCi5ukR8xnyQnWcD7RHdO3ab6G1sl+49dNRpZyg1fCXfDPtt5Aut3CoCQsFNit1chdHsyUJiGDZo72EP/75Bak2dDENcxYyEsbFSqUH4pywhVJwKBgFp1hivwVKjXXrbdxd4x9nwOV4gTwa9QKCGZ+7Fpnbw997nbSfnXMdb7BsujIRvMWwfL4t2RW7GmbTJzUHyO+OtSEvfQjXqWrpiDI/u3GjNVeCMAUWFzVn+0ng8nDVcCVrLyCFfhbWrxfeR7oVkBaXdi6z6suVOa/Vp4hdk0lnsnAoGAcBPoaWr1coMd6+OfSaiPNw3ZlbM9D8cksv1qaNI5AnW0mvP/3J7nQVJz/SCNK9rQSQQdUDJlwjwpPwsuEd4s/jL6qwH7AlhKoq/SCDlndSFn8GxmUWop4Rczhrwiqv69m7qNDMZ4yXtJDgpOnNaql87jKH5oi5fgofSyjcAn8BECgYEAh5aOvUmVHqz+L9WcdU1DWzUo2JvNgOfkOzsCRFQkQq/NOCFofysccmoKjPieSgr7oOyrBCVsYRi2ZYrUfL6nvKkqSjYV94bjEyZthb53Uv3euQmZQjMpPKHFs4ae1rB7RUBjH6JiCTjyd7iTnKem7s9uR/DVeNjZe1lT6LWKlmY="),nil
	return []byte("SHA256withRSA"),nil
}

//生成32位md5字串
func GetMd5String(s string, upper bool, half bool) string {
	h := md5.New()
	h.Write([]byte(s))
	result := hex.EncodeToString(h.Sum(nil))
	if upper == true {
		result = strings.ToUpper(result)
	}
	if half == true {
		result = result[8:24]
	}
	return result
}

//利用随机数生成Guid字串
func UniqueId() string {
	b := make([]byte, 48)
	if _,err := rand.Read(b); err!=nil{
		return ""
	}
	return GetMd5String(base64.URLEncoding.EncodeToString(b), true,false)
}
//func main() {
//	id:=GetSnowflakeId()
//	fmt.Println(id)

//}
