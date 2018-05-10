CI测试：
SequoiaGroup和SequoiaNode下的用例因为会增删节点，因此暂时屏蔽不测。
SequoiaDB中的backupoffline.php用例备份整个集群，执行时间较长，暂时屏蔽不测。
ci测试时指定文件夹而不是具体文件，因此只能跑以Test.php结尾的用例，如authenticateTest.php