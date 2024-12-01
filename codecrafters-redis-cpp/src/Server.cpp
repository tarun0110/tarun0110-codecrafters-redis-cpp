#include <iostream>
#include <cstdlib>
#include <string>
#include <sstream>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <vector>
#include <map>
#include <chrono> //for getting current time
#include <fstream> //creating file
#include <sys/stat.h> // mkdir
#include <cstdint>
using namespace std;

string dir, dbfilename;
int masterFd = -1;
vector<string> decodeStream(istream &stream){
  vector<string>commands;
  char type = stream.get();
  switch(type){
    case '*' :
      string str;
      getline(stream, str, '\r');
      int numberOfElements = stoi(str);
      stream.get();
      while(numberOfElements--){
        stream.get();
        getline(stream, str, '\r');
        int sizeOfElement = stoi(str);
        stream.get();
        str = "";
        while(sizeOfElement--){
          str+=stream.get();
        }
        stream.get();
        stream.get();
        commands.push_back(str);
      }
      break;
  }
  for(string s: commands){
    cout<<s<<" ";
  }
  return commands;
}

vector<string> createDirectory(string filepath){
  vector<string>directories;
  istringstream stream(filepath);
  stream.get();
  string segment;
  while(getline(stream, segment, '/')){
    if(!segment.empty()){
      directories.push_back(segment);
    }
  }
  return directories;
}
uint64_t convertHexStringToInteger(vector<string> hexString, int i) {
    std::vector<uint8_t> bytes;
    std::string byte;

    // Parse the input string and extract each hexadecimal byte
    for(int x=0;x<8;x++) {
        byte = hexString[x+i];
        bytes.push_back(static_cast<uint8_t>(std::stoi(byte, nullptr, 16)));
    }

    // Combine bytes in little-endian order to form the final 32-bit unsigned integer
    uint64_t result = 0;
    for (size_t x = 0; x < 8; ++x) {
        result |= static_cast<uint64_t>(bytes[x]) << (8 * x);
    }

    return result;
}
uint32_t convertHexStringToDecimal(vector<string> hexString, int i) {
    std::vector<uint8_t> bytes;
    std::string byte;

    // Parse the input string and extract each hexadecimal byte
    for(int x;x<i+4;x++) {
        byte = hexString[x];
        bytes.push_back(static_cast<uint8_t>(std::stoi(byte, nullptr, 16)));
    }

    // Combine bytes in little-endian order to form the final 32-bit unsigned integer
    uint32_t result = 0;
    for (size_t x = 0; x < bytes.size(); ++x) {
        result |= static_cast<uint32_t>(bytes[x]) << (8 * x);
    }

    return result;
}
// Convert Unix timestamp to steady_clock::time_point
std::chrono::steady_clock::time_point unixTimeToSteadyClock(uint64_t unixTimeMs) {
    // Get the current system clock time
    auto systemNow = std::chrono::system_clock::now();

    // Convert system clock time to milliseconds since epoch
    auto systemNowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        systemNow.time_since_epoch());

    // Get the current steady clock time
    auto steadyNow = std::chrono::steady_clock::now();

    // Calculate the offset between steady_clock and system_clock
    auto offset = steadyNow - std::chrono::steady_clock::time_point(systemNowMs);

    // Adjust the Unix timestamp to align with steady_clock
    auto steadyTimePoint = std::chrono::steady_clock::time_point(std::chrono::milliseconds(unixTimeMs)) + offset;

    return steadyTimePoint;
}

std::chrono::steady_clock::time_point unixTimeToSteadyClock(uint32_t unixTimeMs) {
    // Get the current system and steady clock times
    auto systemNow = std::chrono::system_clock::now();
    auto steadyNow = std::chrono::steady_clock::now();

    // Calculate the current Unix time in milliseconds
    auto unixNowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        systemNow.time_since_epoch());

    // Compute the offset between steady_clock and system_clock
    auto offset = steadyNow - std::chrono::steady_clock::time_point(std::chrono::milliseconds(unixNowMs.count()));

    // Convert Unix time to steady_clock time
    auto steadyTimePoint = std::chrono::steady_clock::time_point(std::chrono::milliseconds(unixTimeMs)) + offset;

    return steadyTimePoint;
}

string getString(ifstream &inputFile, int len){
  string key = "";
  while(len--){
    string hexString;
    inputFile>>hexString;
    char c = static_cast<char>(stoi(hexString, nullptr, 16));
    key+=c;
  }
  return key;
}

vector<string> readRDBFile(const std::string& filePath) {
     vector<string> hexStrings; // Vector to store hexadecimal strings
    std::ifstream file(filePath, std::ios::binary); // Open file in binary mode
    if (!file) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        return hexStrings;
    }
    unsigned char byte;

    // Read the file byte by byte
    while (file.read(reinterpret_cast<char*>(&byte), sizeof(byte))) {
        // Convert byte to a hexadecimal string
        std::ostringstream oss;
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)byte;
        hexStrings.push_back(oss.str());
    }

    file.close();

    // Print the contents of the vector
    std::cout << "File contents (hexadecimal strings):" << std::endl;
    for (const auto& hexString : hexStrings) {
        std::cout << hexString<< " ";
    }

    std::cout << std::endl;
    return hexStrings;
}

void decodeHexs(vector<string> hexs, map<string, pair<string,chrono::steady_clock::time_point> > &db){
  for(int i=0;i<hexs.size();){
    if(i<9){
      i++;
      continue;
    }
    int len;
    string key="", val="";
    auto current_time = chrono::steady_clock::now();
    auto expiry_time = current_time + chrono::milliseconds(10000000);
    uint64_t result;
    uint32_t result2;
    switch(stoi(hexs[i].c_str(),NULL,16)){
      case 0xFA:
        cout<<"inside FA"<<endl;
        i++;
        if(stoi(hexs[i].c_str(),NULL,16) <= 0x0D) 
          i=i+stoi(hexs[i].c_str(),NULL,16);
        else if(stoi(hexs[i].c_str(),NULL,16) == 0xC0) 
          i++;
        else if(stoi(hexs[i].c_str(),NULL,16) <= 0xC1) 
          i+=2;
        else if(stoi(hexs[i].c_str(),NULL,16) <= 0xC2) 
          i+=4;
        else if(stoi(hexs[i].c_str(),NULL,16) <= 0xC3) 
          i+=8;
        i++;
        if(stoi(hexs[i].c_str(),NULL,16) <= 0x0D) 
          i=i+stoi(hexs[i].c_str(),NULL,16);
        else if(stoi(hexs[i].c_str(),NULL,16) == 0xC0) 
          i++;
        else if(stoi(hexs[i].c_str(),NULL,16) <= 0xC1) 
          i+=2;
        else if(stoi(hexs[i].c_str(),NULL,16) <= 0xC2) 
          i+=4;
        else if(stoi(hexs[i].c_str(),NULL,16) <= 0xC3) 
          i+=8;
        i++;
        break;
      case 0xFE:
        cout<<"inside FE"<<endl;
        i+=2;
        break;
      case 0x00:
        cout<<"inside 00"<<endl;
        i++;
        len = stoi(hexs[i].c_str(),NULL,16);
        i++;
        while(len--){
          key+=static_cast<char>(stoi(hexs[i], nullptr, 16));
          i++;
        }
        len = stoi(hexs[i].c_str(),NULL,16);
        i++;
        while(len--){
          val+=static_cast<char>(stoi(hexs[i], nullptr, 16));
          i++;
        }
        cout<<"key => "<<key<<" val : "<<val<<endl;
        expiry_time = current_time + chrono::milliseconds(10000000);
        db[key] = make_pair(val, expiry_time);
        break;
      case 0xFC:
        cout<<"inside FC"<<endl;
        i++;
        result = convertHexStringToInteger(hexs, i);
        cout<<"timestamp: "<<result<<endl;
        i+=8;
        i++;
        len = stoi(hexs[i].c_str(),NULL,16);
        i++;
        while(len--){
          key+=static_cast<char>(stoi(hexs[i], nullptr, 16));
          i++;
        }
        len = stoi(hexs[i].c_str(),NULL,16);
        i++;
        while(len--){
          val+=static_cast<char>(stoi(hexs[i], nullptr, 16));
          i++;
        }
        cout<<"key => "<<key<<" val : "<<val<<endl;
        expiry_time = unixTimeToSteadyClock(result);
        db[key] = make_pair(val, expiry_time);
        break;
      case 0xFD:
        cout<<"inside FD"<<endl;
        i++;
        result2 = convertHexStringToDecimal(hexs, i);
        i+=4;
        i++;
        len = stoi(hexs[i].c_str(),NULL,16);
        i++;
        while(len--){
          key+=static_cast<char>(stoi(hexs[i], nullptr, 16));
          i++;
        }
        len = stoi(hexs[i].c_str(),NULL,16);
        i++;
        while(len--){
          val+=static_cast<char>(stoi(hexs[i], nullptr, 16));
          i++;
        }
        cout<<"key => "<<key<<" val : "<<val<<endl;
        expiry_time = unixTimeToSteadyClock(result2);
        db[key] = make_pair(val, expiry_time);
        break;
      case 0xFB:
        cout<<"inside FB"<<endl;
        i++;
        i++;
        i++;
        break;
      case 0xFF:
        cout<<"inside FF"<<endl;
        return;
      default:
        cout<<"inside default"<<endl;
        break;
    }
  }
  return;
}
void pingMaster(int masterPort, string masterHost, int port){
  int sockfd, portno, n;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    
    portno = masterPort;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    masterFd = sockfd;
    if (sockfd < 0) 
        cout<<"ERROR opening socket"<<endl;
    server = gethostbyname(masterHost.c_str());
    if (server == NULL) {
        fprintf(stderr,"ERROR, no such host\n");
        exit(0);
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, 
         (char *)&serv_addr.sin_addr.s_addr,
         server->h_length);
    serv_addr.sin_port = htons(portno);
    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) 
        cout<<"ERROR connecting\n";
    printf("Please enter the message: ");
    string resp = "*1\r\n$4\r\nPING\r\n";
    n = write(sockfd,resp.c_str(),strlen(resp.c_str()));
    if (n < 0) 
        cout<<"ERROR writing to socket\n";
    char buffer[256];
    bzero(buffer,256);
    n = read(sockfd,buffer,255);
    if (n < 0) 
      cout<<"ERROR reading from socket\n";
    resp = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"+ to_string(to_string(port).length())+"\r\n"+to_string(port)+"\r\n";
    n = write(sockfd,resp.c_str(),strlen(resp.c_str()));
    if (n < 0) 
        cout<<"ERROR writing to socket\n";
    bzero(buffer,256);
    n = read(sockfd,buffer,255);
    if (n < 0) 
      cout<<"ERROR reading from socket\n";
    resp = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    n = write(sockfd,resp.c_str(),strlen(resp.c_str()));
    if (n < 0) 
        cout<<"ERROR writing to socket\n";
    bzero(buffer,256);
    n = read(sockfd,buffer,255);
    if (n < 0) 
      cout<<"ERROR reading from socket\n";

    resp = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    n = write(sockfd,resp.c_str(),strlen(resp.c_str()));
    if (n < 0) 
        cout<<"ERROR writing to socket\n";
    bzero(buffer,256);
    n = read(sockfd,buffer,255);
    if (n < 0) 
      cout<<"ERROR reading from socket\n";
    close(sockfd);
    return;
}
void propagateToReplicas(vector<pair<string,int> >replicas, string resp){
  for(auto replica: replicas){
    cout<<"Propagating to replica "<<replica.first<<endl;
     int sockfd, portno, n;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    
    portno = replica.second;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    masterFd = sockfd;
    if (sockfd < 0) 
        cout<<"ERROR opening socket"<<endl;
    server = gethostbyname(replica.first.c_str());
    if (server == NULL) {
        fprintf(stderr,"ERROR, no such host\n");
        exit(0);
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, 
         (char *)&serv_addr.sin_addr.s_addr,
         server->h_length);
    serv_addr.sin_port = htons(portno);
    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) 
        cout<<"ERROR connecting\n";
    printf("Please enter the message: ");
    n = write(sockfd,resp.c_str(),strlen(resp.c_str()));
    if (n < 0) 
        cout<<"ERROR writing to socket\n";
  }
  return;
}
int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  int port=6379;
  string role = "master";
  string master_replid;
  string master_repl_offset;
  map<string, pair<string,chrono::steady_clock::time_point> > db;
  for(int i=1;i<argc; i++){
    if (strcmp(argv[i], "--dir")==0){
      dir = argv[++i];
    }
    if (strcmp(argv[i], "--dbfilename")==0){
      dbfilename = argv[++i];
      string filename = dir + "/" + dbfilename;
      vector<string>hexs = readRDBFile(filename);
      decodeHexs(hexs, db);
    }
    if (strcmp(argv[i], "--port")==0){
      port = stoi(argv[++i]);
    }
    if (strcmp(argv[i], "--replicaof")==0){
      role = "slave";
      string masterInfo = argv[++i];
      string masterHost= "";
      int masterPort;
      for(int j=0;j<masterInfo.size();j++){
        if(masterInfo[j]==' ')break;
        masterHost+=masterInfo[j];
      }
      masterPort = stoi(masterInfo.substr(masterHost.length()+1, masterInfo.length()-masterHost.length()-1));
      cout<<"host "<<masterHost<<" port "<<masterPort<<endl;
      pingMaster(masterPort, masterHost, port);
    }
  }
  if(role=="master"){
    master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    master_repl_offset="0";
  }
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port"<<port<<"\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  fd_set read_fds;
  vector<int>client_fds;
  vector<pair<string, int> >replicas;
  string slaveHost = "localhost";
  int slavePort;
  while(true){
    FD_ZERO(&read_fds);
    FD_SET(server_fd,&read_fds);
    int max_fd = server_fd;

    for(int fd: client_fds){
      FD_SET(fd, &read_fds);
      if(fd>max_fd) max_fd = fd;
    }

    int activity = select(max_fd+1, &read_fds, NULL, NULL, NULL);
    if(activity < 0){
      std::cerr << "select error\n";
      break;
    }

    if(FD_ISSET(server_fd, &read_fds)){
      struct sockaddr_in client_addr;
      int client_addr_len = sizeof(client_addr);
      int new_client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *) &client_addr_len);
      if(new_client_fd < 0){
        std::cerr << "accept error\n";
        break;
      }
      cout<<"New client connected with FD: "<<new_client_fd<<"\n";
      client_fds.push_back(new_client_fd); 
    }

    for(auto it = client_fds.begin(); it != client_fds.end();){
      int client_fd = *it;
      if (FD_ISSET(client_fd, &read_fds)) {
        char buffer[256];
        memset(buffer, 0, sizeof(buffer));
        int n = read(client_fd, buffer, sizeof(buffer) - 1);
        if (n <= 0) {
            std::cerr << "Client disconnected: FD = " << client_fd << "\n";
            // close(client_fd);
            it = client_fds.erase(it); // Remove from the list
            continue;
        }
        std::cout << "Received message from client FD " << client_fd << ": " << buffer;
        istringstream stream(string(buffer, n));
        vector<string> commands = decodeStream(stream);
        for(string command : commands) cout<<"command "<<command<<" ";
        cout<<"Helklow sdlkfj "<<endl;
        if(strcasecmp(commands[0].c_str(),"ping") == 0)
          send(client_fd, "+PONG\r\n", 7, 0);
        else if(strcasecmp(commands[0].c_str(),"echo") == 0){
          string resp =  "+" + commands[1] + "\r\n";
          send(client_fd, resp.c_str() , resp.length(), 0);
        }
        else if(strcasecmp(commands[0].c_str(),"set") == 0){
          auto current_time = chrono::steady_clock::now();
          if(commands.size()==5 && strcasecmp(commands[3].c_str(),"px") == 0){
            auto expiry_time = current_time + chrono::milliseconds(stoi(commands[4]));
            db[commands[1]] = make_pair(commands[2],expiry_time);
          }
          else{
            auto max_time = current_time + chrono::milliseconds(10000000);
            db[commands[1]] = make_pair(commands[2], max_time);
          } 
          cout<<"set in db "<<commands[1]<<":"<<db[commands[1]].first<<endl;
          cout<<"clientfd "<<client_fd<<" masterfd "<<masterFd<<endl;
          if(client_fd != masterFd)
            propagateToReplicas(replicas, string(buffer, n));
          if(client_fd != masterFd)
            send(client_fd, "+OK\r\n", 5, 0);
        }
        else if(strcasecmp(commands[0].c_str(),"get") == 0){
          auto current_time = chrono::steady_clock::now();
          if(db[commands[1]].second < current_time){
            string resp = "$-1\r\n";
            send(client_fd, resp.c_str(), resp.length(), 0);
          }
          else {
            string resp = "+" + db[commands[1]].first + "\r\n";
            cout<<"resp: "<<resp<<endl;
            send(client_fd, resp.c_str(), resp.length(), 0);
          }
        }
        else if(strcasecmp(commands[0].c_str(),"config") == 0){
          if(strcasecmp(commands[1].c_str(),"get") == 0){
            string resp = "";
            if(strcasecmp(commands[2].c_str(),"dir") == 0){
              resp = "*2\r\n$3\r\ndir\r\n$" + to_string(dir.length()) +"\r\n"+ dir +"\r\n";
            }
            else if(strcasecmp(commands[2].c_str(),"dbfilename") == 0){
              resp = "*2\r\n$10\r\ndbfilename\r\n$" + to_string(dbfilename.length()) +"\r\n"+ dbfilename +"\r\n";
            }
            cout<<"resp2: "<<resp<<endl;
            send(client_fd, resp.c_str(), resp.length(), 0);
          }
        }
        else if(strcasecmp(commands[0].c_str(),"keys") == 0){
          vector<string>keys;
          if(commands[1] == "*"){
            for(auto it = db.begin();it!=db.end();it++){
              cout<<"Key: "<<it->first << " value: "<<it->second.first<<endl;
              keys.push_back(it->first);
            }
            string resp="*";
            resp+=to_string(keys.size()) + "\r\n";
            for(string key:keys){
              resp+="$"+to_string(key.length())+"\r\n";
              resp+=key+"\r\n";
            }
            send(client_fd, resp.c_str(), resp.length(), 0);
          }
        }else if(strcasecmp(commands[0].c_str(),"info") == 0){
          string resp="";
          string value = "role:"+role + "\nmaster_replid:"+master_replid+"\nmaster_repl_offset:"+master_repl_offset;
          resp ="$"+ to_string(value.length()) + "\r\n";
          resp+=value;
          resp+="\r\n"; 
          send(client_fd, resp.c_str(), resp.length(), 0);
        }else if(strcasecmp(commands[0].c_str(),"replconf") == 0){
          if(strcasecmp(commands[1].c_str(),"listening-port") == 0){
            slavePort = stoi(commands[2]);
          }
          string resp="+OK\r\n";
          send(client_fd, resp.c_str(), resp.length(), 0);
        }else if(strcasecmp(commands[0].c_str(),"psync") == 0){
          string resp="+FULLRESYNC "+master_replid+" "+master_repl_offset+"\r\n";
          send(client_fd, resp.c_str(), resp.length(), 0);
          string empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
          resp = "$"+to_string(empty_rdb.length())+"\r\n"+empty_rdb;
          cout<<"replica port "<<slavePort<<endl;
          replicas.push_back(make_pair(slaveHost, slavePort));
          send(client_fd, resp.c_str(), resp.length(), 0);
        }
      }
      ++it;
    }
  }

  for (int fd : client_fds) {
    close(fd);
  }
  return 0;


  // struct sockaddr_in client_addr;
  // int client_addr_len = sizeof(client_addr);
  // std::cout << "Waiting for a client to connect...\n";
  // while(1){
  //   int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
  //     if(client_fd < 0){
  //       cout<<"accept failed";
  //       std::cerr << "accept failed\n";
  //       break;
  //     }
  //   cout << "Client connected\n";
  //   int pid = fork(); //creating a thread (new process)
  //   if(pid == 0){
  //     //handling of child process (Individual Client handling, can be shifted to a new function)
  //     while(1){
  //       char buffer[256];
  //       memset(buffer, 0, 256);
  //       int n = read(client_fd, buffer, 255);
  //       if(n<0)
  //         break;
  //       if(strcasecmp(buffer,"*1\r\n$4\r\nping\r\n") !=0 ){
  //         continue;
  //       }
  //       send(client_fd, "+PONG\r\n",7, 0);
  //     }
  //     exit(0);
  //   }
  //   close(client_fd);
  // }
  // close(server_fd);
  // return 0;
}

