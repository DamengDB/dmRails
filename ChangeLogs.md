# dmRails

Ruby on Rails是基于Ruby的Web应用程序框架，dmRails是DM提供的Ruby on Rails连接DM数据库的驱动，当前版本为 `1.0.2`  ，目前用于适配 `5.2` 以及 `6.0` 版本的Ruby on Rails。

## ChangeLogs

#### dmRails v1.0.2(2024-10-30)

* 新增了对于 `6.0` 版本的Ruby on Rails支持，修复了部分函数功能实现错误的问题

#### dmRails v1.0.1(2024-10-12)

* 修复了查询 `json` 与 `jsonb` 类型列的定义出错的问题
* 修复了大小写敏感的数据库，ails迁移文件中使用rename_column修改列名，修改后的列名存在大小写不匹配的问题
* 修复了使用默认主键创建时， `save` 插入数据时报错的问题

#### dmRails v1.0.0(2024-08-09)

* 新建项目，适配Ruby on Rails框架 `5.2` 版本
