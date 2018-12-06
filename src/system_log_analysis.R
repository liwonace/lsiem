library(elastic)
options(java.parameters = "-Xmx2g")

trim <- function (x) gsub("^\\s+|\\s+$", "", x)

ip_list <- c("192.168.7.70","192.168.7.71","31.13.82.1","52.165.171.165","64.233.188.188")
file_list <- c("image.jpeg.jpg")
vol_id <- c("405A4F58-7A88-11E8-BA3E-1831BFB301A9")

win10_func = function(str,tmp_msg){
  tmp_time <- substr(tmp_msg,regexpr("Date:",tmp_msg)+6,regexpr("Event ID:",tmp_msg)-2)
  tmp_time2 <- substr(tmp_time,1,13)
  tmp_id <- substr(tmp_msg,regexpr("Event ID:",tmp_msg)+10,regexpr("Task:",tmp_msg)-3)
  tmp_msg2 <- substr(tmp_msg,regexpr("Description:",tmp_msg)+13,nchar(tmp_msg))
  tmp_lv <- substr(tmp_msg,regexpr("Level:",tmp_msg)+7,regexpr("Opcode:",tmp_msg)-2)
  tmp_pc <- substr(tmp_msg,regexpr("Computer:",tmp_msg)+10,regexpr("Description:",tmp_msg)-2)
  #print(tmp_lv)
  win10_time <<- c(win10_time,tmp_time)
  win10_hour <<- c(win10_hour,tmp_time2)
  win10_eventid <<- c(win10_eventid,tmp_id)
  win10_lv <<- c(win10_lv,tmp_lv)
  win10_pc <<- c(win10_pc,tmp_pc)
  win10_messages <<- c(win10_messages,tmp_msg)
  win10_sep <<- c(win10_sep,str)
  
  #if(tmp_id == '1001'){
  #  event_name <- substr(tmp_msg,regexpr("이벤트 이름:",tmp_msg)+8,regexpr("응답:",tmp_msg)-1)
  #  if(event_name == "APPCRASH"){
  #    app_time <<- c(app_time,tmp_time)
  #    app_name <<- c(app_name,substr(tmp_msg,regexpr("P1:",tmp_msg)+4,regexpr("P2:",tmp_msg)-1))
  #  }
  #}else 
  if(tmp_id == '5145'){
    tmp_action <- substr(tmp_msg,regexpr("액세스:",tmp_msg)+5,regexpr("액세스 검사 결과:",tmp_msg)-2)
    tmp_file <- substr(tmp_msg,regexpr("상대 대상 이름:",tmp_msg)+10,regexpr("액세스 요청 정보:",tmp_msg)-1)
    tmp_ip <- substr(tmp_msg,regexpr("원본 주소:",tmp_msg)+8,regexpr("원본 포트:",tmp_msg)-2)
    tmp_dir <- substr(tmp_msg,regexpr("공유 경로:",tmp_msg)+8,regexpr("상대 대상 이름:",tmp_msg)-2)
    ip_check <- 0
    
    for(ip in ip_list){
      if(ip == tmp_ip){
        ip_check <- 1
        break
      }
    }
    #print(tmp_action)
    if((regexpr("DELETE",tmp_action) > 0 || regexpr("WriteData",tmp_action) > 0 || regexpr("ReadData",tmp_action) > 0 || regexpr("ReadAttributes",tmp_action) > 0) && tmp_file != "\\" && ip_check == 0 && tmp_dir != ""){
      tmp_action <- strsplit(tmp_action,"\t")
      tmp_action <- unique(unlist(tmp_action))
      set_action <- NULL
      check <- 0
      write_check <- 0
      read_check <- 0
      delete_check <- 0
      file_check <- 0
      inj_check <- 0
      html_inj_check <- 0
      
      for(f in file_list){
        if(tmp_file == f){
          file_check <- 1
          break
        }
      }
      
      if(regexpr("System32",tmp_dir) > 0 && (regexpr(".dll",tmp_file) > 0 || regexpr(".DLL",tmp_file) > 0)){
        inj_check <- 1
      }
      if(regexpr(".php",tmp_file) > 0 || regexpr(".PHP",tmp_file) > 0 || regexpr(".HTM",tmp_file) > 0 || regexpr(".HTM",tmp_file) > 0){
        html_inj_check <- 1
      }
      
      for(a in 1:length(tmp_action)){
        if(tmp_action[a] != ""){
          
          if(regexpr("WriteData",tmp_action[a]) > 0){
            write_check <- 1
          }else if(regexpr("ReadData",tmp_action[a]) > 0 || regexpr("ReadAttributes",tmp_action[a]) > 0){
            read_check <- 1
          }else if(regexpr("DELETE",tmp_action[a]) > 0){
            delete_check <- 1
          }
          
          if(check == 0){
            set_action <- tmp_action[a]
            check <- check + 1
          }else{
            set_action <- paste0(paste0(set_action,","),tmp_action[a])
          }
        }
      }
      if(write_check == 1 || delete_check == 1){
        access_ip1 <<- c(access_ip,tmp_ip)
        access_dir1 <<- c(access_dir,tmp_dir)
        access_file1 <<- c(access_file,tmp_file)
        access_time1 <<- c(access_time,tmp_time)
        action1 <<- c(action,set_action)
      }
      if(read_check == 1){
        access_ip2 <<- c(access_ip,tmp_ip)
        access_dir2 <<- c(access_dir,tmp_dir)
        access_file2 <<- c(access_file,tmp_file)
        access_time2 <<- c(access_time,tmp_time)
        action2 <<- c(action,set_action)
      }
      if(write_check == 1 && read_check == 1){
        access_ip3 <<- c(access_ip,tmp_ip)
        access_dir3 <<- c(access_dir,tmp_dir)
        access_file3 <<- c(access_file,tmp_file)
        access_time3 <<- c(access_time,tmp_time)
        action3 <<- c(action,set_action)
      }
      if(read_check == 1 && file_check == 1){
        access_ip4 <<- c(access_ip,tmp_ip)
        access_dir4 <<- c(access_dir,tmp_dir)
        access_file4 <<- c(access_file,tmp_file)
        access_time4 <<- c(access_time,tmp_time)
        action4 <<- c(action,set_action)
      }
      if(inj_check == 1){
        inj_time <<- c(inj_time,tmp_time)
        inj_ip <<- c(inj_ip,tmp_ip)
        inj_dir <<- c(inj_dir,tmp_dir)
        inj_file <<- c(inj_file,tmp_file)
        inj_action <<- c(inj_action,set_action)
      }
      if(html_inj_check == 1){
        html_inj_time <<- c(html_inj_time,tmp_time)
        html_inj_ip <<- c(html_inj_ip,tmp_ip)
        html_inj_dir <<- c(html_inj_dir,tmp_dir)
        html_inj_file <<- c(html_inj_file,tmp_file)
        html_inj_action <<- c(html_inj_action,set_action)
      }
      
      #print(tmp_ip)
      #print(tmp_dir)
      #print(tmp_file)
      #print(set_action)
    }
  }
  
  if(regexpr("ntfs",str) != -1){
    vol_check <- 0
    tmp_vol_id <- NULL
    
    if(tmp_id == "142"){
      tmp_vol_id <- trim(substr(tmp_msg,regexpr("볼륨 GUID:",tmp_msg)+9,regexpr("볼륨 이름:",tmp_msg)-2))
    }else if(tmp_id == "145"){
      tmp_vol_id <- trim(substr(tmp_msg,regexpr("볼륨 ID:",tmp_msg)+7,regexpr("볼륨 이름:",tmp_msg)-2))
    }
    
    if(!is.null(tmp_vol_id)){
      print(tmp_vol_id)
      
      tmp_vol_id <- substr(tmp_vol_id,2,nchar(tmp_vol_id)-1)
      
      for(vol in vol_id){
        if(vol == tmp_vol_id){
          vol_check <- 1
        }
      }
      
      win10_ntfs_time <<- c(win10_ntfs_time, tmp_time)
      win10_ntfs_hour <<- c(win10_ntfs_hour, tmp_time2)
      win10_ntfs_pc <<- c(win10_ntfs_pc,tmp_pc)
      win10_ntfs_id <<- c(win10_ntfs_id,tmp_vol_id)
      win10_ntfs_nm <<- c(win10_ntfs_nm,trim(substr(tmp_msg,regexpr("볼륨 이름:",tmp_msg)+7,regexpr("부팅 볼륨:",tmp_msg)-2)))
      win10_ntfs_boot <<- c(win10_ntfs_boot,trim(substr(tmp_msg,regexpr("부팅 볼륨:",tmp_msg)+7,regexpr("부팅 볼륨:",tmp_msg)+11)))
      
      if(vol_check == 1){
        limit_vol_time <<- c(limit_vol_time, tmp_time)
        limit_vol_pc <<- c(limit_vol_pc,tmp_pc)
        limit_vol_id <<- c(limit_vol_id,tmp_vol_id)
        limit_vol_nm <<- c(limit_vol_nm,trim(substr(tmp_msg,regexpr("볼륨 이름:",tmp_msg)+7,regexpr("부팅 볼륨:",tmp_msg)-2)))
        limit_vol_boot <<- c(limit_vol_boot,trim(substr(tmp_msg,regexpr("부팅 볼륨:",tmp_msg)+7,regexpr("부팅 볼륨:",tmp_msg)+11)))
      }
    }
  }
}

sql_func = function(tmp_msg){
  tmp_sql <- strsplit(tmp_msg,"~/")
  tmp_sql <- unlist(tmp_sql)
  
  if(tmp_sql[6] == "0"){
    limit_ip_time <<- c(limit_ip_time,tmp_sql[1])
    limit_ip <<- c(limit_ip,tmp_sql[3])
    limit_ip_sql <<- c(limit_ip_sql,tmp_sql[2])
  }
  if(tmp_sql[7] == "1"){
    limit_table_time <<- c(limit_table_time,tmp_sql[1])
    limit_table_ip <<- c(limit_table_ip,tmp_sql[3])
    limit_table_sql <<- c(limit_table_sql,tmp_sql[2])
  }
  if(tmp_sql[8] == "1"){
    inj_sql_time <<- c(inj_sql_time,tmp_sql[1])
    inj_sql_ip <<- c(inj_sql_ip,tmp_sql[3])
    inj_sql <<- c(inj_sql,tmp_sql[2])
  }
}

cent_func = function(str,tmp_time,tmp_body){
  ymdt <- NULL
  year <- substr(as.character(Sys.Date()),1,4)
  time_spl <- strsplit(tmp_time," ")
  time_spl <- unlist(time_spl)
  switch (time_spl[1],
          Jan = {ymdt <- paste0(year,"-01-")},
          Feb = {ymdt <- paste0(year,"-02-")},
          Mar = {ymdt <- paste0(year,"-03-")},
          Apr = {ymdt <- paste0(year,"-04-")},
          May = {ymdt <- paste0(year,"-05-")},
          Jun = {ymdt <- paste0(year,"-06-")},
          Jul = {ymdt <- paste0(year,"-07-")},
          Aug = {ymdt <- paste0(year,"-08-")},
          Sep = {ymdt <- paste0(year,"-09-")},
          Oct = {ymdt <- paste0(year,"-10-")},
          Nov = {ymdt <- paste0(year,"-11-")},
          Dec = {ymdt <- paste0(year,"-12-")}
  )
  ymdt <- paste0(ymdt,paste0(time_spl[2],paste0(" ",time_spl[3])))
  
  cent_time <<- c(cent_time,ymdt)
  cent_sep <<- c(cent_sep,str)
  body_spl <- strsplit(trim(tmp_body)," ")
  body_spl <- unlist(body_spl)
  
  cent_messages <<- c(cent_messages,gsub(body_spl[2],"",gsub(body_spl[1],"",tmp_body)))
  tmp_host <- body_spl[1]
  cent_host <<- c(cent_host,tmp_host)
  if(regexpr("\\[",body_spl[2]) != -1){
    tmp_pro <- strsplit(body_spl[2],"\\[")
    tmp_pro <- unlist(tmp_pro)
    cent_prog <<- c(cent_prog,tmp_pro[1])
  }else{
    cent_prog <<- c(cent_prog,gsub(":","",body_spl[2]))
  }
  
  if(regexpr("secure",str) != -1){
    if(regexpr("Accepted",tmp_body) != -1){
      cent_con_time <<- c(cent_con_time,ymdt)
      if(regexpr("\\[",tmp_body) != -1){
        cent_con_pid <<- c(cent_con_pid,substr(tmp_body,regexpr("\\[",tmp_body)+1,regexpr("\\]",tmp_body)-1))
      }else{
        cent_con_pid <<- c(cent_con_pid,"")
      }
      tmp_ip <- substr(tmp_body,regexpr("from",tmp_body)+5,regexpr("port",tmp_body)-2)
      cent_con_ip <<- c(cent_con_ip,tmp_ip)
      cent_con_id <<- c(cent_con_id,substr(tmp_body,regexpr("for",tmp_body)+4,regexpr("from",tmp_body)-2))
      cent_con_state <<- c(cent_con_state,"open")
      cent_con_host <<- c(cent_con_host,tmp_host)
    }else if(regexpr("session opened",tmp_body) != -1){
      cent_con_time <<- c(cent_con_time,ymdt)
      if(regexpr("\\[",tmp_body) != -1){
        cent_con_pid <<- c(cent_con_pid,substr(tmp_body,regexpr("\\[",tmp_body)+1,regexpr("\\]",tmp_body)-1))
      }else{
        cent_con_pid <<- c(cent_con_pid,"")
      }
      cent_con_ip <<- c(cent_con_ip,"")
      cent_con_state <<- c(cent_con_state,paste0("open ",substr(tmp_body,regexpr("by",tmp_body),nchar(tmp_body))))
      cent_con_id <<- c(cent_con_id,substr(tmp_body,regexpr("user",tmp_body)+5,regexpr("by",tmp_body)-2))
      cent_con_host <<- c(cent_con_host,tmp_host)
    }else if(regexpr("session closed",tmp_body) != -1){
      cent_con_time <<- c(cent_con_time,ymdt)
      if(regexpr("\\[",tmp_body) != -1){
        cent_con_pid <<- c(cent_con_pid,substr(tmp_body,regexpr("\\[",tmp_body)+1,regexpr("\\]",tmp_body)-1))
      }else{
        cent_con_pid <<- c(cent_con_pid,"")
      }
      cent_con_ip <<- c(cent_con_ip,"")
      cent_con_state <<- c(cent_con_state,"close")
      cent_con_id <<- c(cent_con_id,substr(tmp_body,regexpr("user",tmp_body)+5,nchar(tmp_body)))
      cent_con_host <<- c(cent_con_host,tmp_host)
    }
  }
}

while(1){
  system("/home/elastic/max_result_window_set.sh",ignore.stdout = TRUE)
  conn <- connect(es_host="192.168.7.71" ,es_port=9200,errors = "complete")
  exist <- alias_exists(alias="search_autocomplete")
  index <- index_stats()
  index <- names(index$indices)
  t_date <- Sys.Date()
  t_date <- gsub("-",".",t_date)
  last_count <- NULL
  last_str <- NULL
  read_last <- NULL
  file_path <- "/home/elastic/R_source/last_his.csv"
  
  cent_result <- NULL
  cent_time <- NULL
  cent_messages <- NULL
  cent_sep <- NULL
  cent_prog <- NULL
  cent_host <- NULL
  cent_con_time <- NULL
  cent_con_pid <- NULL
  cent_con_ip <- NULL
  cent_con_id <- NULL
  cent_con_state <- NULL
  cent_con_result <- NULL
  cent_con_host <- NULL
 
  app_time <- NULL
  app_name <- NULL
  
  inj_time <- NULL
  inj_dir <- NULL
  inj_file <- NULL
  inj_action <- NULL
  inj_ip <- NULL
  
  html_inj_time <- NULL
  html_inj_dir <- NULL
  html_inj_file <- NULL
  html_inj_action <- NULL
  html_inj_ip <- NULL
  
  limit_ip_time <- NULL
  limit_ip <- NULL
  limit_ip_sql <- NULL
  limit_table_time <- NULL
  limit_table_ip <- NULL
  limit_table_sql <- NULL
  inj_sql_time <- NULL
  inj_sql_ip <- NULL
  inj_sql <- NULL
  
  access_ip1 <- NULL
  access_dir1 <- NULL
  access_file1 <- NULL
  action1 <- NULL
  access_time1 <- NULL
  access_ip2 <- NULL
  access_dir2 <- NULL
  access_file2 <- NULL
  action2 <- NULL
  access_time2 <- NULL
  access_ip3 <- NULL
  access_dir3 <- NULL
  access_file3 <- NULL
  action3 <- NULL
  access_time3 <- NULL
  access_ip4 <- NULL
  access_dir4 <- NULL
  access_file4 <- NULL
  action4 <- NULL
  access_time4 <- NULL
  
  win10_result <- NULL
  win10_time <- NULL
  win10_hour <- NULL
  win10_messages <- NULL
  win10_eventid <- NULL
  win10_lv <- NULL
  win10_sep <- NULL
  win10_pc <- NULL
  win10_ntfs_time <- NULL
  win10_ntfs_hour <- NULL
  win10_ntfs_id <- NULL
  win10_ntfs_nm <- NULL
  win10_ntfs_boot <- NULL
  win10_ntfs_pc <- NULL
  
  limit_vol_time <- NULL
  limit_vol_id <- NULL
  limit_vol_nm <- NULL
  limit_vol_boot <- NULL
  limit_vol_pc <- NULL
  count <- 0
  out <- NULL
  
  if(file.exists(file_path)){
    read_last <- read.csv(file_path)
  }
  
  for(str in index){
    start_num <- 0
    
    if(regexpr("kibana",str) == -1 && regexpr(as.character(t_date),str) != -1){
      if(!is.null(read_last)){
        for(i in 1:length(read_last[,1])){
          if(str == as.character(read_last[i,2])){
            start_num <- read_last[i,3]
          }
        }
      }
      out <- Search(index = str,size = 1000, from = start_num, sort = '@timestamp')$hits
      count <- out$total
      
      last_str <- c(last_str,str)
      last_count <- c(last_count,count)
      count <- count - start_num
      
      if(count != 0){
        if(count <= 1000){
          for(i in 1:count){
            tmp <- out$hits[[i]]
            tmp_msg <- tmp$`_source`$message
            
            if(regexpr("cent",str) >= 0){
              tmp_time <- substr(tmp_msg,1,15)
              tmp_body <- substr(tmp_msg,17,nchar(tmp_msg))
              cent_func(str,tmp_time,tmp_body)
            }else if(regexpr("win10",str) >= 0){
              win10_func(str,tmp_msg)
            }else if(regexpr("sql-data",str) >= 0){
              sql_func(tmp_msg)
            }
          }
        }else{
          while (count != 0){
            if(count > 1000){
              for(i in 1:1000){
                tmp <- out$hits[[i]]
                tmp_msg <- tmp$`_source`$message
                if(regexpr("cent",str) >= 0){
                  tmp_time <- substr(tmp_msg,1,15)
                  tmp_body <- substr(tmp_msg,17,nchar(tmp_msg))
                  cent_func(str,tmp_time,tmp_body)
                }else if(regexpr("win10",str) >= 0){
                  win10_func(str,tmp_msg)
                }else if(regexpr("sql-data",str) >= 0){
                  sql_func(tmp_msg)
                }
              }
              count <- count - 1000
              start_num <- start_num + 1000
            }else{
              for(j in 1:count){
                tmp <- out$hits[[j]]
                tmp_msg <- tmp$`_source`$message
                if(regexpr("cent",str) >= 0){
                  tmp_time <- substr(tmp_msg,1,15)
                  tmp_body <- substr(tmp_msg,17,nchar(tmp_msg))
                  cent_func(str,tmp_time,tmp_body)
                }else if(regexpr("win10",str) >= 0){
                  win10_func(str,tmp_msg)
                }else if(regexpr("sql-data",str) >= 0){
                  sql_func(tmp_msg)
                }
                count <- count - 1
              }
            }
            out <- Search(index = str,size = 1000, from = start_num, sort = '@timestamp')$hits
          }
        }
      }
    }
  }
  
  win10_result <- cbind(win10_time,win10_hour,win10_eventid,win10_lv,win10_pc,win10_messages,win10_sep)
  win10_ntfs_result <- cbind(win10_ntfs_time,win10_ntfs_hour,win10_ntfs_pc,win10_ntfs_id,win10_ntfs_nm,win10_ntfs_boot)
  cent_con_result <- cbind(cent_con_time,cent_con_host,cent_con_ip,cent_con_pid,cent_con_id,cent_con_state)
  cent_result <- cbind(cent_time,cent_host,cent_prog,cent_messages,cent_sep)
  limit_vol_result <- cbind(limit_vol_time,limit_vol_id,limit_vol_nm,limit_vol_boot,limit_vol_pc)
  
  win_access_result1 <- cbind(access_time1,access_dir1,access_file1,access_ip1,action1)
  win_access_result2 <- cbind(access_time2,access_dir2,access_file2,access_ip2,action2)
  win_access_result3 <- cbind(access_time3,access_dir3,access_file3,access_ip3,action3)
  win_access_result4 <- cbind(access_time4,access_dir4,access_file4,access_ip4,action4)
  win_inj_result <- cbind(inj_time,inj_dir,inj_file,inj_ip,inj_action)
  win_html_inj_result <- cbind(html_inj_time,html_inj_dir,html_inj_file,html_inj_ip,html_inj_action)
  
  limit_ip_result <- cbind(limit_ip_time,limit_ip,limit_ip_sql)
  limit_table_result <- cbind(limit_table_time,limit_table_ip,limit_table_sql)
  inj_sql_result <- cbind(inj_sql_time,inj_sql_ip,inj_sql)
  
  win10_result <- data.frame(win10_result)
  win10_ntfs_result <- data.frame(win10_ntfs_result)
  cent_con_result <- data.frame(cent_con_result)
  cent_result <- data.frame(cent_result)
  limit_vol_result <- data.frame(limit_vol_result)
  
  win_access_result1 <- data.frame(win_access_result1)
  win_access_result2 <- data.frame(win_access_result2)
  win_access_result3 <- data.frame(win_access_result3)
  win_access_result4 <- data.frame(win_access_result4)
  win_inj_result <- data.frame(win_inj_result)
  win_html_inj_result <- data.frame(win_html_inj_result)
  
  limit_ip_result <- data.frame(limit_ip_result)
  limit_table_result <- data.frame(limit_table_result)
  inj_sql_result <- data.frame(inj_sql_result)
  
  if((length(cent_result) != 0) && (length(cent_result[,1]) != 0)){
    invisible(docs_bulk(cent_result,"result_cent",chunk_size = 1000000))
  }
  if((length(win10_result) != 0) && (length(win10_result[,1]) != 0)){
    invisible(docs_bulk(win10_result,"result_win",chunk_size = 1000000))
  }
  if((length(limit_vol_result) != 0) && (length(limit_vol_result[,1]) != 0)){
    invisible(docs_bulk(limit_vol_result,"result_limit_vol",chunk_size = 1000000))
  }
  if((length(win10_ntfs_result) != 0) && (length(win10_ntfs_result[,1]) != 0)){
    invisible(docs_bulk(win10_ntfs_result,"result_win_ntfs",chunk_size = 1000000))
  }
  if((length(cent_con_result) != 0) && (length(cent_con_result[,1]) != 0)){
    invisible(docs_bulk(cent_con_result,"result_cent_con",chunk_size = 1000000))
  }
  if((length(win_access_result1) != 0) && (length(win_access_result1[,1]) != 0)){
    invisible(docs_bulk(win_access_result1,"result_access1",chunk_size = 1000000))
  }
  if((length(win_access_result2) != 0) && (length(win_access_result2[,1]) != 0)){
    invisible(docs_bulk(win_access_result2,"result_access2",chunk_size = 1000000))
  }
  if((length(win_access_result3) != 0) && (length(win_access_result3[,1]) != 0)){
    invisible(docs_bulk(win_access_result3,"result_access3",chunk_size = 1000000))
  }
  if((length(win_access_result4) != 0) && (length(win_access_result4[,1]) != 0)){
    invisible(docs_bulk(win_access_result4,"result_access4",chunk_size = 1000000))
  }
  if((length(win_inj_result) != 0) && (length(win_inj_result[,1]) != 0)){
    invisible(docs_bulk(win_inj_result,"result_inj",chunk_size = 1000000))
  }
  if((length(win_html_inj_result) != 0) && (length(win_html_inj_result[,1]) != 0)){
    invisible(docs_bulk(win_html_inj_result,"result_html_inj",chunk_size = 1000000))
  }
  if((length(limit_ip_result) != 0) && (length(limit_ip_result[,1]) != 0)){
    invisible(docs_bulk(limit_ip_result,"limit_ip_sql",chunk_size = 1000000))
  }
  if((length(limit_table_result) != 0) && (length(limit_table_result[,1]) != 0)){
    invisible(docs_bulk(limit_table_result,"limit_table_sql",chunk_size = 1000000))
  }
  if((length(inj_sql_result) != 0) && (length(inj_sql_result[,1]) != 0)){
    invisible(docs_bulk(inj_sql_result,"inj_sql",chunk_size = 1000000))
  }
  
  tmp_read_nm <- NULL
  tmp_read_count <- NULL
  
  if((!is.null(read_last)) && (length(read_last) != 0) && (length(last_str) != 0)){
    for(j in 1:length(read_last[,1])){
      if(regexpr(substr(as.character(Sys.Date()),1,4),as.character(read_last[j,2])) != -1){
        tmp_read_nm <- c(tmp_read_nm,as.character(read_last[j,2]))
        tmp_read_count <- c(tmp_read_count,read_last[j,3])
      }
    }
    
    for(i in 1:length(last_str)){
      check <- 0
      for(j in 1:length(tmp_read_nm)){
        if(last_str[i] == tmp_read_nm[j]){
          tmp_read_count[j] <- last_count[i]
          check <- 1
        }
      }
      if(check == 0){
        tmp_read_nm <- c(tmp_read_nm,last_str[i])
        tmp_read_count <- c(tmp_read_count,last_count[i])
      }
    }
    last_his <- cbind.data.frame(tmp_read_nm,tmp_read_count)
  }else{
    last_his <- cbind.data.frame(last_str,last_count)
  }
  
  if(length(last_count) != 0){
    write.csv(last_his,file_path,append = TRUE)
  }
  
  Sys.sleep(10)
}






