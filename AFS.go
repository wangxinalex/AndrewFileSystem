package main
import (
	"fmt"
	"io"
    "os"
	"time"
	"crypto/md5"
)

const max_client_num int = 10
const max_file_num int = 10

type ClientFile struct {
	file_uid string
	file_name string
	callback byte
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
			client_fi, _ = os.Open(client_path + file_name)
		}
		temp_clientFile := ClientFile{}
		temp_clientFile.file_uid = uid
		temp_clientFile.file_name = file_name
		temp_clientFile.callback = 0
		client_file_map[file_name] = temp_clientFile

		return client_fi
	}
	if client_file_info.callback == 1 {
		// TODO reach latest file
		ServerFetch(client_file_info.file_uid)
		client_fi, _ := os.Open(client_path + file_name)
		return client_fi
	}
	client_fi, _ := os.Open(client_path + file_name)
	return client_fi
}

var ClientClose = func(file_name string, fi *os.File) {
	fi.Close()
	client_file_info, _ := client_file_map[file_name]
	if(client_file_info.callback == 1) {
		// TODO reach latest file
		ServerFetch(client_file_info.file_uid)
	} else {
		// TODO send to server
		ServerStore(client_file_info.file_uid)
	}
}

var ServerCreate = func(file_name string) string {
	fi, _ := os.Create(server_path + file_name)
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
	
	server_file_map[uid] = temp_serverFile

	return uid
}

var ServerFetch = func(file_uid string) {
	file_name := server_file_map[file_uid].file_name

	client_fi, _ := os.Create(client_path + file_name)
	server_fi, _ := os.Open(server_path + file_name)
	defer client_fi.Close()
	defer server_fi.Close()
	io.Copy(client_fi, server_fi)
}

var ServerStore = func(file_uid string) {
	file_name := server_file_map[file_uid].file_name

	client_fi, _ := os.Open(client_path + file_name)
	server_fi, _ := os.Create(server_path + file_name)
	defer client_fi.Close()
	defer server_fi.Close()
	io.Copy(server_fi, client_fi)
}

var ServerRemove = func(file_uid string) {
	file_name := server_file_map[file_uid].file_name
	os.Remove(server_path + file_name)
	delete(client_file_map, file_uid)
}
var Hello = func() {
	fmt.Println("Hello World!")
}

func main() {
	Hello()
	ServerInit()
	ClientInit()
	fi := ClientOpen("aaa.txt", "a+")
	fi.WriteString("Just a test!\r\n")
	ClientClose("aaa.txt", fi)
	/*
	os.Remove(client_path+"aaa.txt")
	delete(client_file_map, "aaa.txt")
	ClientOpen("aaa.txt", "a+")
	*/
}