###jar包地址
/home/student1/lichengjun

###运行命令
java -jar hbaseapi-1.0.20220308-jar-with-dependencies.jar emr-worker-2,emr-worker1,emr-header-1

###结果展示,删除插入的数据lichengjun2
ROW                               COLUMN+CELL                                                                                     
 Jack                             column=info:class, timestamp=2022-03-20T20:45:46.656, value=2                                   
 Jack                             column=info:student_id, timestamp=2022-03-20T20:45:46.656, value=20210000000003                 
 Jack                             column=score:programming, timestamp=2022-03-20T20:45:46.656, value=80                           
 Jack                             column=score:understanding, timestamp=2022-03-20T20:45:46.656, value=80                         
 Jerry                            column=info:class, timestamp=2022-03-20T20:45:46.656, value=1                                   
 Jerry                            column=info:student_id, timestamp=2022-03-20T20:45:46.656, value=20210000000002                 
 Jerry                            column=score:programming, timestamp=2022-03-20T20:45:46.656, value=67                           
 Jerry                            column=score:understanding, timestamp=2022-03-20T20:45:46.656, value=85                         
 Rose                             column=info:class, timestamp=2022-03-20T20:45:46.656, value=2                                   
 Rose                             column=info:student_id, timestamp=2022-03-20T20:45:46.656, value=20210000000004                 
 Rose                             column=score:programming, timestamp=2022-03-20T20:45:46.656, value=61                           
 Rose                             column=score:understanding, timestamp=2022-03-20T20:45:46.656, value=60                         
 Tom                              column=info:class, timestamp=2022-03-20T20:45:46.656, value=1                                   
 Tom                              column=info:student_id, timestamp=2022-03-20T20:45:46.656, value=20210000000001                 
 Tom                              column=score:programming, timestamp=2022-03-20T20:45:46.656, value=82                           
 Tom                              column=score:understanding, timestamp=2022-03-20T20:45:46.656, value=75                         
 lichengjun                       column=info:class, timestamp=2022-03-20T20:45:46.656, value=1                                   
 lichengjun                       column=info:student_id, timestamp=2022-03-20T20:45:46.656, value=G20220735020181                
 lichengjun                       column=score:programming, timestamp=2022-03-20T20:45:46.656, value=100                          
 lichengjun                       column=score:understanding, timestamp=2022-03-20T20:45:46.656, value=100 
  