# adviser-parent
scala编写的sparkjob，包括以下工程。
##adviser-orign-toes
从kafka消费日志，写入elasticsearch。
##adviser-eventflow-firststage
读取elasticsearch数据，使用累加器计算日志信息，并写回elasticsearch。
##adviser-exchange-import
监听redis缓存，读取redis大量数据，高并发写入数据库
##adviser-crm-import
读取指定路径的csv文件，并通过spark sql 导入到指定的数据库和表
