library(elastic)
options(java.parameters = "-Xmx2g")

trim <- function (x) gsub("^\\s+|\\s+$", "", x)
problem_id <- c("4688","5140","4946","4947")

win10_func = function(str,tmp_msg){
  tmp_time <- substr(tmp_msg,regexpr("Date:",tmp_msg)+6,regexpr("Event ID:",tmp_msg)-2)
  tmp_time2 <- substr(tmp_time,1,13)
  tmp_id <- substr(tmp_msg,regexpr("Event ID:",tmp_msg)+10,regexpr("Task:",tmp_msg)-2)
  tmp_msg2 <- substr(tmp_msg,regexpr("Description:",tmp_msg)+13,nchar(tmp_msg))
  tmp_lv <- substr(tmp_msg,regexpr("Level:",tmp_msg)+7,regexpr("Opcode:",tmp_msg)-2)
  tmp_pc <- substr(tmp_msg,regexpr("Computer:",tmp_msg)+10,regexpr("Description:",tmp_msg)-2)
  print(tmp_lv)
  win10_time <<- c(win10_time,tmp_time)
  win10_hour <<- c(win10_hour,tmp_time2)
  win10_eventid <<- c(win10_eventid,tmp_id)
  win10_lv <<- c(win10_lv,tmp_lv)
  win10_pc <<- c(win10_pc,tmp_pc)
  win10_messages <<- c(win10_messages,tmp_msg)
  win10_sep <<- c(win10_sep,str)
  for(id in problem_id){
    if(id == tmp_id){
      win10_pb_time <<- c(win10_pb_time,tmp_time)
      win10_pb_id <<- c(win10_pb_id,tmp_id)
      win10_pb_msg <<- c(win10_pb_msg,tmp_msg2)
      win10_pb_lv <<- c(win10_pb_lv,tmp_lv)
      win10_pb_pc <<- c(win10_pb_pc,tmp_pc)
    }
  }
  if(regexpr("ntfs",str) != -1){
    win10_ntfs_time <<- c(win10_ntfs_time, tmp_time)
    win10_ntfs_hour <<- c(win10_ntfs_hour, tmp_time2)
    win10_ntfs_pc <<- c(win10_ntfs_pc,tmp_pc)
    win10_ntfs_id <<- c(win10_ntfs_id,trim(substr(tmp_msg,regexpr("ID:",tmp_msg)+4,regexpr("볼륨 이름:",tmp_msg)-2)))
    win10_ntfs_nm <<- c(win10_ntfs_nm,trim(substr(tmp_msg,regexpr("볼륨 이름:",tmp_msg)+7,regexpr("부팅 볼륨:",tmp_msg)-2)))
    win10_ntfs_boot <<- c(win10_ntfs_boot,trim(substr(tmp_msg,regexpr("부팅 볼륨:",tmp_msg)+7,regexpr("부팅 볼륨:",tmp_msg)+11)))
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
  win10_pb_time <- NULL
  win10_pb_id <- NULL
  win10_pb_msg <- NULL
  win10_pb_pc <- NULL
  win10_pb_lv <- NULL
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
      out <- Search(index = str,size = 1000, from = start_num)$hits
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
                }
                count <- count - 1
              }
            }
            out <- Search(index = str,size = 1000, from = start_num)$hits
          }
        }
      }
    }
  }
  
  win10_result <- cbind(win10_time,win10_hour,win10_eventid,win10_lv,win10_pc,win10_messages,win10_sep)
  win10_ntfs_result <- cbind(win10_ntfs_time,win10_ntfs_hour,win10_ntfs_pc,win10_ntfs_id,win10_ntfs_nm,win10_ntfs_boot)
  win10_pb_result <- cbind(win10_pb_time,win10_pb_pc,win10_pb_id,win10_pb_lv,win10_pb_msg)
  cent_con_result <- cbind(cent_con_time,cent_con_host,cent_con_ip,cent_con_pid,cent_con_id,cent_con_state)
  cent_result <- cbind(cent_time,cent_host,cent_prog,cent_messages,cent_sep)
  
  win10_result <- data.frame(win10_result)
  win10_ntfs_result <- data.frame(win10_ntfs_result)
  win10_pb_result <- data.frame(win10_pb_result)
  cent_con_result <- data.frame(cent_con_result)
  cent_result <- data.frame(cent_result)
  
  if((length(cent_result) != 0) && (length(cent_result[,1]) != 0)){
    invisible(docs_bulk(cent_result,"result_cent",chunk_size = 1000000))
  }
  if((length(win10_result) != 0) && (length(win10_result[,1]) != 0)){
    invisible(docs_bulk(win10_result,"result_win",chunk_size = 1000000))
  }
  if((length(win10_ntfs_result) != 0) && (length(win10_ntfs_result[,1]) != 0)){
    invisible(docs_bulk(win10_ntfs_result,"result_win_ntfs",chunk_size = 1000000))
  }
  if((length(win10_pb_result) != 0) && (length(win10_pb_result[,1]) != 0)){
    invisible(docs_bulk(win10_pb_result,"result_win_pb",chunk_size = 1000000))
  }
  if((length(cent_con_result) != 0) && (length(cent_con_result[,1]) != 0)){
    invisible(docs_bulk(cent_con_result,"result_cent_con",chunk_size = 1000000))
  }
  
  
  tmp_read_nm <- NULL
  tmp_read_count <- NULL
  
  if((!is.null(read_last)) && (length(read_last) != 0)){
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
  
  Sys.sleep(2)
}






