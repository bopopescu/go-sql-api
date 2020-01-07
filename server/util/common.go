package util

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"github.com/shiyongabc/go-sql-api/server/lib"
	"golang.org/x/sys/windows"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
	"fmt"
	"github.com/shiyongabc/jwt-go"
	"unsafe"
	"github.com/StackExchange/wmi"
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
	mid, _ = strconv.ParseInt(GetPhysicalID(), 10, 64)
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
		return id
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
func GetPhysicalID() string{
	var ids []string
	if guid,err := getMachineGuid(); err != nil{
		panic(err.Error())
	}else{
		ids = append(ids, guid)
	}
	if cpuinfo,err := getCPUInfo();err != nil && len(cpuinfo) > 0 {
		panic(err.Error())
	}else{
		ids = append(ids, cpuinfo[0].VendorID+cpuinfo[0].PhysicalID)
	}
	if mac,err := getMACAddress();err!=nil{
		panic(err.Error())
	}else{
		ids = append(ids, mac)
	}
	sort.Strings(ids)
	idsstr := strings.Join(ids, "|/|")
	return GetMd5String(idsstr,true,true)
}

func getMACAddress() (string, error){
	netInterfaces, err := net.Interfaces()
	if err != nil {
		panic(err.Error())
	}
	mac,macerr := "",errors.New("无法获取到正确的MAC地址")
	for i := 0; i < len(netInterfaces); i++ {
		//fmt.Println(netInterfaces[i])
		if (netInterfaces[i].Flags & net.FlagUp) != 0 && (netInterfaces[i].Flags & net.FlagLoopback) == 0{
			addrs, _ := netInterfaces[i].Addrs()
			for _, address := range addrs {
				ipnet, ok := address.(*net.IPNet)
				//fmt.Println(ipnet.IP)
				if  ok && ipnet.IP.IsGlobalUnicast() {
					// 如果IP是全局单拨地址，则返回MAC地址
					mac = netInterfaces[i].HardwareAddr.String()
					return mac,nil
				}
			}
		}
	}
	return mac,macerr
}

type cpuInfo struct {
	CPU        int32    `json:"cpu"`
	VendorID   string   `json:"vendorId"`
	PhysicalID string   `json:"physicalId"`
}

type win32_Processor struct {
	Manufacturer              string
	ProcessorID               *string
}

func getCPUInfo() ([]cpuInfo, error) {
	var ret []cpuInfo
	var dst []win32_Processor
	q := wmi.CreateQuery(&dst, "")
	fmt.Println(q)
	if err := wmiQuery(q, &dst); err != nil {
		return ret, err
	}

	var procID string
	for i, l := range dst {
		procID = ""
		if l.ProcessorID != nil {
			procID = *l.ProcessorID
		}

		cpu := cpuInfo{
			CPU:        int32(i),
			VendorID:   l.Manufacturer,
			PhysicalID: procID,
		}
		ret = append(ret, cpu)
	}

	return ret, nil
}

// WMIQueryWithContext - wraps wmi.Query with a timed-out context to avoid hanging
func wmiQuery(query string, dst interface{}, connectServerArgs ...interface{}) error {
	ctx := context.Background()
	if _, ok := ctx.Deadline(); !ok {
		ctxTimeout, cancel := context.WithTimeout(ctx, 3000000000)//超时时间3s
		defer cancel()
		ctx = ctxTimeout
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- wmi.Query(query, dst, connectServerArgs...)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

func getMachineGuid() (string, error) {
	// there has been reports of issues on 32bit using golang.org/x/sys/windows/registry, see https://github.com/shirou/gopsutil/pull/312#issuecomment-277422612
	// for rationale of using windows.RegOpenKeyEx/RegQueryValueEx instead of registry.OpenKey/GetStringValue
	var h windows.Handle
	err := windows.RegOpenKeyEx(windows.HKEY_LOCAL_MACHINE, windows.StringToUTF16Ptr(`SOFTWARE\Microsoft\Cryptography`), 0, windows.KEY_READ|windows.KEY_WOW64_64KEY, &h)
	if err != nil {
		return "", err
	}
	defer windows.RegCloseKey(h)

	const windowsRegBufLen = 74 // len(`{`) + len(`abcdefgh-1234-456789012-123345456671` * 2) + len(`}`) // 2 == bytes/UTF16
	const uuidLen = 36

	var regBuf [windowsRegBufLen]uint16
	bufLen := uint32(windowsRegBufLen)
	var valType uint32
	err = windows.RegQueryValueEx(h, windows.StringToUTF16Ptr(`MachineGuid`), nil, &valType, (*byte)(unsafe.Pointer(&regBuf[0])), &bufLen)
	if err != nil {
		return "", err
	}

	hostID := windows.UTF16ToString(regBuf[:])
	hostIDLen := len(hostID)
	if hostIDLen != uuidLen {
		return "", fmt.Errorf("HostID incorrect: %q\n", hostID)
	}

	return hostID, nil
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
