package main
import (
	"fmt"
	"io"
	"os"
	"time"
	"crypto/md5"
	"bufio"
	"strings"
)

const max_client_num int = 10
const max_file_num int = 10

type ClientFile struct {
	file_uid string
	file_name string
	callback byte
	file_fd *os.File
}

type ServerClient struct {
	client_id int
}


type ServerFile struct {
	file_valid byte
	file_uid string
	file_name string
	promise map[int] int // client_id -> client_id
}

var client_path string = "D:/workspace/Go/Client/"
var server_path string = "D:/workspace/Go/Server/"

var server_file_map map[string] ServerFile // uid -> file_name
var server_client_map map[int] ServerClient // client_id -> client
var client_num int = 0

var client_file_map map[string] ClientFile // file_name -> uid
var client_id int

var ServerInit = func() {
	server_client_map = make(map[int] ServerClient)
	server_file_map = make(map[string] ServerFile)
}

var ClientInit = func() {
	client_file_map = make(map[string] ClientFile)
	client_id = NewClient()
}

var NewClient = func() int {
	client_num++
	temp_client := ServerClient{}
	temp_client.client_id = client_num
	server_client_map[client_num] = temp_client
	return client_num
}

var getServerFileUIDByFileName = func(file_name string) string {
	for key, value := range server_file_map {
		if value.file_name == file_name {
			return key
		}
	}
	return ""
}

var ClientOpen = func(file_name, mode string) *os.File {
	client_file_info, ok := client_file_map[file_name]
	if !ok {
		// TODO tell server to create file
		var uid string = getServerFileUIDByFileName(file_name)
		var client_fi *os.File
		_, ok := server_file_map[uid]
		if !ok {
			uid = ServerCreate(file_name)
			client_fi, _ = os.Create(client_path + file_name)
		} else {
			ServerFetch(uid)
			client_fi, _ = os.OpenFile(client_path + file_name, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0x777)
		}
		temp_clientFile := ClientFile{}
		temp_clientFile.file_uid = uid
		temp_clientFile.file_name = file_name
		temp_clientFile.callback = 0
		temp_clientFile.file_fd = client_fi
		client_file_map[file_name] = temp_clientFile

		return client_fi
	}
	if client_file_info.callback == 1 {
		// TODO reach latest file
		ServerFetch(client_file_info.file_uid)
		client_fi, err := os.OpenFile(client_path + file_name, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0x777)
		if err != nil {
				fmt.Println("ClientOpen: Open client file error 2!")
			}
		client_file_info.file_fd = client_fi
		client_file_map[file_name] = client_file_info
		return client_fi
	}
	client_fi, err := os.OpenFile(client_path + file_name, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0x777)
	if err != nil {
		fmt.Println("ClientOpen: Open client file error 3!")
	}
	client_file_info.file_fd = client_fi
	client_file_map[file_name] = client_file_info
	return client_fi
}

var ClientRead = func(file_name string) string {
	file := client_file_map[file_name].file_fd
	br := bufio.NewReader(file)
	var str string = ""
	for {
		line, isPrefix, err := br.ReadLine()
		if err != nil {
			//fmt.Println(err)
			break
		}
		if isPrefix {
			fmt.Println("A too long line, seems unexpected.")
			return ""
		}
		str = str + string(line) + "\n"
	}
	fmt.Println(str)
	return str
}

var ClientWrite = func(file_name, data string) {
	file := client_file_map[file_name].file_fd
	_, err := file.WriteString(data+"\n")
	if err != nil {
		fmt.Println("ClientWrite: write file error")
	}
}

var ClientClose = func(file_name string) {
	client_file_info, err := client_file_map[file_name]
	if !err {
		fmt.Println("ClientClose: Cannot find file!")
	}
	fi := client_file_info.file_fd
	fi.Close()
	if(client_file_info.callback == 1) {
		// TODO reach latest file
		ServerFetch(client_file_info.file_uid)
		client_file_info.callback = 0
	} else {
		// TODO send to server
		ServerStore(client_file_info.file_uid)
	}
	client_file_info.file_fd = nil
	client_file_map[file_name] = client_file_info
}

var ServerCreate = func(file_name string) string {
	fi, err := os.Create(server_path + file_name)
	if err != nil {
		fmt.Println("ServerCreate: Create server file error!")
	}
	fi.Close()
	Md5Inst := md5.New()
	Md5Inst.Write([]byte(file_name))
	Md5Result := Md5Inst.Sum([]byte(""))
	TimeResult := time.Now().UnixNano()

	uid := fmt.Sprintf("%x%d", Md5Result, TimeResult)

	temp_serverFile := ServerFile{}
	temp_serverFile.promise = make(map[int] int)
	temp_serverFile.file_uid = uid
	temp_serverFile.file_name = file_name
	temp_serverFile.promise[client_id] = client_id
	temp_serverFile.file_valid = 0
	
	server_file_map[uid] = temp_serverFile

	return uid
}

var ServerFetch = func(file_uid string) {
	file_name := server_file_map[file_uid].file_name

	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerFetch: Cannot find server file!")
	}

	server_file_info.promise[client_id] = client_id
	server_file_info.file_valid = 0
	server_file_map[file_name] = server_file_info

	client_fi, _ := os.Create(client_path + file_name)
	server_fi, _ := os.Open(server_path + file_name)
	io.Copy(client_fi, server_fi)
	client_fi.Close()
	server_fi.Close()
}

var ServerStore = func(file_uid string) {
	file_name := server_file_map[file_uid].file_name

	client_fi, _ := os.Open(client_path + file_name)
	server_fi, _ := os.Create(server_path + file_name)
	io.Copy(server_fi, client_fi)
	client_fi.Close()
	server_fi.Close()
}

var ServerRemove = func(file_uid string) {
	file_name := server_file_map[file_uid].file_name
	os.Remove(server_path + file_name)
	delete(client_file_map, file_uid)
}

var ServerSetLock = func(file_uid string) {
	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerSetLock: Cannot find client file!")
	}
	server_file_info.file_valid = 0
	server_file_map[server_file_info.file_name] = server_file_info
}

var ServerReleaseLock = func(file_uid string) {
	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerReleaseLock: Cannot find client file!")
	}
	server_file_info.file_valid = 1
	server_file_map[server_file_info.file_name] = server_file_info
}

var ServerRemoveCallback = func(file_uid string) {
	for key, _ := range server_file_map[file_uid].promise {
		if key != client_id {
			// TODO send callback to client
			file_name := server_file_map[file_uid].file_name
			client_file_info, _ := client_file_map[file_name]
			client_file_info.callback = 1
			client_file_map[file_name] = client_file_info
		}
	}
}

var ServerBreakCallback = func(file_uid string) {
	
}

var Hello = func() {
	fmt.Println("Hello World!")
}

var ClientRoutine = func() {
	ClientInit()
	r := bufio.NewReader(os.Stdin)
	for {
		b, _, _ := r.ReadLine()
		line := string(b)
		//fmt.Println(line)
		tokens := strings.Split(line, " ")
		switch tokens[0] {
			case "open":
				ClientOpen(tokens[1], "a+")
				break
			case "close":
				ClientClose(tokens[1])
				break
			case "read":
				ClientRead(tokens[1])
				break
			case "write":
				ClientWrite(tokens[1], tokens[2])
				break
		}
	}
}

func main() {
	Hello()
	ServerInit()
	/*
	
	ClientInit()
	fi := ClientOpen("aaa.txt", "a+")
	fi.WriteString("Just a test!\r\n")
	ClientClose("aaa.txt")
	
	os.Remove(client_path+"aaa.txt")
	delete(client_file_map, "aaa.txt")
	ClientOpen("aaa.txt", "a+")
	*/
	ClientRoutine()
}