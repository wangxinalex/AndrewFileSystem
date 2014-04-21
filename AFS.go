package main
import (
	"fmt"
	"io"
	"os"
	"time"
	"crypto/md5"
	"bufio"
	"strings"
	"strconv"
)

// max client number
const max_client_num int = 10
// max file number
const max_file_num int = 10

type ClientFile struct {
	file_uid string // uid
	file_name string
	callback byte // if the file is old, it it set to 1
	file_fd *os.File // file descriptor
}

type ServerClient struct {
	client_id int
	client_chan chan string // the channel to communicate to the client
}


type ServerFile struct {
	file_valid int // if it is locked, it it set to the client_id; else it is 0
	file_lock_mode string // exclusive or shared
	file_uid string
	file_name string
	promise map[int] int // client_id -> client_id
}

var client_path string = "D:/workspace/Go/Client/"
var server_path string = "D:/workspace/Go/Server/"
var log_path string = "D:/workspace/Go/"

// for server
var server_file_map map[string] ServerFile // uid -> file_name
var server_client_map map[int] ServerClient // client_id -> client
var client_num int = 0
var server_chan chan string

// for client
var client_file_map map[string] ClientFile // file_name -> uid
var client_id int // current client id
var client_chan chan string

var log_file *os.File

// help method to look at the status when debugging, so it is private
func printStatus() {
	fmt.Println(server_file_map)
	fmt.Println(server_client_map)
	fmt.Println(client_num)
	fmt.Println(client_file_map)
	fmt.Println(client_id)
}

func ServerInit() {
	server_client_map = make(map[int] ServerClient)
	server_file_map = make(map[string] ServerFile)
	server_chan = make(chan string, 1024)

	log_file, _ = os.OpenFile(log_path + "log.txt", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0x777)
}

func ClientInit() {
	client_file_map = make(map[string] ClientFile)
	client_chan = make(chan string, 1024)

	client_id = NewClient(client_chan)
}

func NewClient(client_chan chan string) int {
	client_num++
	temp_client := ServerClient{}
	temp_client.client_id = client_num
	temp_client.client_chan = client_chan
	server_client_map[client_num] = temp_client
	return client_num
}

// search the file uid by file name in server part
func getServerFileUIDByFileName(file_name string) string {
	for key, value := range server_file_map {
		if value.file_name == file_name {
			return key
		}
	}
	return ""
}

// create a file
func ClientCreate(file_name string) *os.File {
	go func() {
		server_chan <- "Create " + strconv.Itoa(client_id) + " " + file_name
		server_chan <- "Create " + strconv.Itoa(client_id) + " " + file_name
	}()

	// get the uid
	var uid string = <-client_chan
	client_fi, _ := os.Create(client_path + file_name)

	temp_clientFile := ClientFile{}
	temp_clientFile.file_uid = uid
	temp_clientFile.file_name = file_name
	temp_clientFile.callback = 0
	temp_clientFile.file_fd = client_fi
	client_file_map[file_name] = temp_clientFile

	return client_fi
}

func ClientOpen(file_name string) *os.File {
	client_file_info, ok := client_file_map[file_name]
	if !ok {
		// TODO tell server to create file

		// it is tricky here, because we do not know whether to create or fetch the file.
		go func() {
			server_chan <- "FetchOrCreate " + strconv.Itoa(client_id) + " " + file_name
			server_chan <- "FetchOrCreate " + strconv.Itoa(client_id) + " " + file_name
		}()
		var command string = <-client_chan
		tokens := strings.Split(command, " ")
		types := tokens[0] // fetch or create
		uid := tokens[1]
		var client_fi *os.File
		switch types {
			case "create":
				client_fi, _ = os.Create(client_path + file_name)
				break
			case "fetch":
				client_fi, _ = os.OpenFile(client_path + file_name, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0x777)
				break
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
		go func() {
			server_chan <- "Fetch " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
			server_chan <- "Fetch " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
		}()
		<-client_chan
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

func ClientRead(file_name string) string {
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
		str = str + string(line)
	}
	fmt.Println(str)
	return str
}

func ClientWrite(file_name, data string) {
	file := client_file_map[file_name].file_fd
	_, err := file.WriteString(data+"\n")
	if err != nil {
		fmt.Println("ClientWrite: write file error")
	}
}

func ClientClose(file_name string) {
	client_file_info, err := client_file_map[file_name]
	if !err {
		fmt.Println("ClientClose: Cannot find file!")
	}
	fi := client_file_info.file_fd
	fi.Close()
	if(client_file_info.callback == 1) {
		// TODO reach latest file
		go func() {
			server_chan <- "Fetch " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
			server_chan <- "Fetch " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
		}()
		<-client_chan
		client_file_info.callback = 0
	} else {
		// TODO send to server
		go func() {
			server_chan <- "Store " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
			server_chan <- "Store " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
		}()
		<-client_chan
	}
	client_file_info.file_fd = nil
	client_file_map[file_name] = client_file_info
}

func ClientDelete(file_name string) {
	go func() {
		server_chan <- "Delete " + strconv.Itoa(client_id) + " " + file_name
		server_chan <- "Delete " + strconv.Itoa(client_id) + " " + file_name
	}()
	<- client_chan
	os.Remove(client_path + file_name)
	delete(client_file_map, file_name)
}

func ClientSetLock(file_name, lock_mode string) {
	client_file_info, err := client_file_map[file_name]
	if !err {
		fmt.Println("ClientSetLock: Cannot find file!")
	}
	go func() {
		server_chan <- "SetLock " + strconv.Itoa(client_id) + " " + client_file_info.file_uid + " " + lock_mode
		server_chan <- "SetLock " + strconv.Itoa(client_id) + " " + client_file_info.file_uid + " " + lock_mode
	}()
	<- client_chan
}

func ClientUnsetLock(file_name string) {
	client_file_info, err := client_file_map[file_name]
	if !err {
		fmt.Println("ClientUnsetLock: Cannot find file!")
	}
	go func() {
		server_chan <- "UnsetLock " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
		server_chan <- "UnsetLock " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
	}()
	<- client_chan
}

func ClientRemoveCallback(file_name string) {
	client_file_info, err := client_file_map[file_name]
	if !err {
		fmt.Println("ClientRemoveCallback: Cannot find file!")
	}
	go func() {
		server_chan <- "RemoveCallback " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
		server_chan <- "RemoveCallback " + strconv.Itoa(client_id) + " " + client_file_info.file_uid
	}()
	<- client_chan
}

func ServerFetchOrCreate(client_id int, file_name string) (string, string) {
	var uid string = getServerFileUIDByFileName(file_name)
	_, ok := server_file_map[uid]
	if !ok {
		uid = ServerCreate(file_name)
		return uid, "create"
	} else {
		ServerFetch(client_id, uid)
		return uid, "fetch"
	}
}

func ServerCreate(file_name string) string {
	fi, err := os.Create(server_path + file_name)
	if err != nil {
		fmt.Println("ServerCreate: Create server file error!")
	}
	fi.Close()
	Md5Inst := md5.New()
	Md5Inst.Write([]byte(file_name))
	Md5Result := Md5Inst.Sum([]byte(""))
	TimeResult := time.Now().UnixNano()

	// uid = sha5 + time
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

func ServerFetch(client_id int, file_uid string) {
	file_name := server_file_map[file_uid].file_name

	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerFetch: Cannot find server file!")
	}

	// if has exclusive lock, we could not fetch
	if server_file_info.file_valid > 0 && server_file_info.file_valid != client_id && server_file_info.file_lock_mode == "exclusive" {
		fmt.Println("ServerFetch: Exclusive lock set, cannot fetch!")
		return
	}

	server_file_info.promise[client_id] = client_id
	server_file_map[file_name] = server_file_info

	client_fi, _ := os.Create(client_path + file_name)
	server_fi, _ := os.Open(server_path + file_name)
	io.Copy(client_fi, server_fi)
	client_fi.Close()
	server_fi.Close()
}

func ServerStore(client_id int, file_uid string) {
	file_name := server_file_map[file_uid].file_name

	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerStore: Cannot find server file!")
	}

	// if has lock, we could not fetch
	if server_file_info.file_valid > 0 && server_file_info.file_valid != client_id && server_file_info.file_lock_mode == "exclusive" {
		fmt.Println("ServerFetch: Exclusive lock set, cannot store!")
		return
	}

	if server_file_info.file_valid > 0 && server_file_info.file_valid != client_id && server_file_info.file_lock_mode == "shared" {
		fmt.Println("ServerFetch: Shared lock set, cannot store!")
		return
	}

	client_fi, _ := os.Open(client_path + file_name)
	server_fi, _ := os.Create(server_path + file_name)
	io.Copy(server_fi, client_fi)
	client_fi.Close()
	server_fi.Close()

	// promise callback
	setCallback(file_uid)
}

func ServerRemove(file_uid string) {
	file_name := server_file_map[file_uid].file_name
	os.Remove(server_path + file_name)
	delete(server_file_map, file_uid)
}

func ServerSetLock(client_id int, file_uid, lock_mode string) {
	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerSetLock: Cannot find client file!")
	}

	if(server_file_info.file_valid > 0) {
		fmt.Println("ServerSetLock: Already locked!")
		return
	}
	switch lock_mode {
		case "exclusive":
			for key, _ := range server_file_map[file_uid].promise {
				if key != client_id {
					fmt.Println("ServerSetLock: Other maintain copies, cannot set exclusive lock!")
					return
				}
			}
			break
	}
	server_file_info.file_valid = client_id
	server_file_info.file_lock_mode = lock_mode
	server_file_map[server_file_info.file_name] = server_file_info
}

func ServerReleaseLock(client_id int, file_uid string) {
	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerReleaseLock: Cannot find client file!")
	}
	server_file_info.file_valid = 0
	server_file_map[server_file_info.file_name] = server_file_info
}

func setCallback(file_uid string) {
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

func ServerRemoveCallback(client_id int, file_uid string) {
	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerRemoveCallback: Cannot find client file!")
	}
	delete(server_file_info.promise, client_id)
	server_file_map[file_uid] = server_file_info
}

func ServerBreakCallback(file_uid string) {
	server_file_info, ok := server_file_map[file_uid]
	if !ok {
		fmt.Println("ServerBreakCallback: Cannot find client file!")
	}
	server_file_info.promise = make(map[int] int)
	server_file_map[file_uid] = server_file_info
}

func Hello() {
	fmt.Println("Hello World!")
}

func ClientRoutine() {
	ClientInit()
	r := bufio.NewReader(os.Stdin)
	for {
		b, _, _ := r.ReadLine()
		line := string(b)
		log_file.WriteString(line+"\n")
		//fmt.Println(line)
		tokens := strings.Split(line, " ")
		switch tokens[0] {
			case "create":
				ClientCreate(tokens[1])
				break
			case "delete":
				ClientDelete(tokens[1])
				break
			case "open":
				ClientOpen(tokens[1])
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
			case "setLock":
				ClientSetLock(tokens[1], tokens[2])
				break
			case "unsetLock":
				ClientUnsetLock(tokens[1])
				break
			case "removeCallback":
				ClientRemoveCallback(tokens[1])
				break
			case "status":
				printStatus()
				break
			case "quit":
				log_file.Close()
				goto END
				break
		}
	}
	END:
}
func ServerRoutine() {
	for {
		select {
			case <- server_chan:
				line := <-server_chan
				fmt.Println(line)
				tokens := strings.Split(line, " ")
				switch tokens[0] {
					case "FetchOrCreate":
						i, _ := strconv.Atoi(tokens[1])
						uid, types := ServerFetchOrCreate(i, tokens[2])
						go func() {
							server_client_map[i].client_chan <- types + " " + uid
							server_client_map[i].client_chan <- types + " " + uid
						}()
						break
					case "Create":
						uid := ServerCreate(tokens[2])
						i, _ := strconv.Atoi(tokens[1])
						go func() {
							server_client_map[i].client_chan <- uid
							server_client_map[i].client_chan <- uid
						}()
						break
					case "Fetch":
						i, _ := strconv.Atoi(tokens[1])
						ServerFetch(i, tokens[2])
						go func() {
							server_client_map[i].client_chan <- "ok"
							server_client_map[i].client_chan <- "ok"
						}()
						break
					case "Store":
						i, _ := strconv.Atoi(tokens[1])
						ServerStore(i, tokens[2])
						go func() {
							server_client_map[i].client_chan <- "ok"
							server_client_map[i].client_chan <- "ok"
						}()
						break
					case "Delete":
						uid := ServerCreate(tokens[2])
						i, _ := strconv.Atoi(tokens[1])
						ServerRemove(uid)
						go func() {
							server_client_map[i].client_chan <- "ok"
							server_client_map[i].client_chan <- "ok"
						}()
						break
					case "SetLock":
						i, _ := strconv.Atoi(tokens[1])
						ServerSetLock(i, tokens[2], tokens[3])
						go func() {
							server_client_map[i].client_chan <- "ok"
							server_client_map[i].client_chan <- "ok"
						}()
						break
					case "UnsetLock":
						i, _ := strconv.Atoi(tokens[1])
						ServerReleaseLock(i, tokens[2])
						go func() {
							server_client_map[i].client_chan <- "ok"
							server_client_map[i].client_chan <- "ok"
						}()
						break
					case "RemoveCallback":
						i, _ := strconv.Atoi(tokens[1])
						ServerRemoveCallback(i, tokens[2])
						go func() {
							server_client_map[i].client_chan <- "ok"
							server_client_map[i].client_chan <- "ok"
						}()
						break
				}

		}
		
	}
}
func main() {
	Hello()
	ServerInit()
	go ServerRoutine()
	ClientRoutine()
}