# adviser-parent
scala编写的sparkjob，包括以下工程。
##adviser-orign-toes
使用spark kafka streaming 消费通过 logtrace 产生的微服务直接调用的日志，计算成事件流的格式，写入elasticsearch。
##adviser-crm-import
读取指定路径的csv文件，并通过spark sql 导入到指定的数据库和表
