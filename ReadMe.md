# DmRails

此目录文件为适配ruby on rails框架，支持5.1与6.0版本。



## 主要功能

支持的功能包括连接数据库，执行语句，绑定参数执行语句以及获取结果集等功能。



## 使用方法

使用时需要在已经安装了ruby on rails框架的ruby目录下将此目录中文件移动到对应文件至对应位置并修改部分代码，步骤如下：
  1、将activerecord目录下的 dm目录 以及 dm_adapter.rb文件 复制至 activerecord-*.*.*.*/lib/active_record/connection_adapters目录 下

  2、将activerecord目录下的 dm_database_tasks.rb文件 复制至 activerecord-*.*.*.*/lib/active_record/tasks目录 下

  3、修改 activerecord-*.*.*.*/lib/active_record/tasks目录 下的代码：
	如果为5.1版本找到
			register_task(/mysql/,        ActiveRecord::Tasks::MySQLDatabaseTasks)
			register_task(/postgresql/,   ActiveRecord::Tasks::PostgreSQLDatabaseTasks)
			register_task(/sqlite/,       ActiveRecord::Tasks::SQLiteDatabaseTasks)
	在此部分代码下添加一行 register_task(/dm/,           ActiveRecord::Tasks::DmDatabaseTasks)
	
	如果为6.0版本找到
			register_task(/mysql/,        "ActiveRecord::Tasks::MySQLDatabaseTasks")
			register_task(/postgresql/,   "ActiveRecord::Tasks::PostgreSQLDatabaseTasks")
			register_task(/sqlite/,       "ActiveRecord::Tasks::SQLiteDatabaseTasks")
	在此部分代码下添加一行 register_task(/dm/,           "ActiveRecord::Tasks::DmDatabaseTasks")
	格式保持一致即可

  4、将apartment目录下的 dm_adapter.rb文件 复制至 apartment-*.*.*/lib/apartment/adapters目录 下

  5、如果使用了ruby的migration_comments库，测试中使用版本为0.4.1版本，请将migration_comments目录下的 dm_adapter.rb文件 复制至migration_comments-0.4.1/lib/migration_comments/active_record/connection_adapters目录下，
        同时修改migration_comments-0.4.1/lib目录下的migration_comments.rb文件，在self.setup中找到adapters = %w(PostgreSQL Mysql2 SQLite)，将其改为adapters = %w(PostgreSQL Mysql2 SQLite Dm)即可