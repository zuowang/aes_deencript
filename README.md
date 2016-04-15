### How to use? ###
cd aes-deencrypt

hive -i 

hive>  select hex(aes_encrypt('ABC', '1234567890123456'));

OK

CBA4ACFB309839BA426E07D67F23564F

hive> select aes_decrypt(unhex('CBA4ACFB309839BA426E07D67F23564F'), '1234567890123456');

OK

ABC

### persistent the UDF ###

 create function aes_decrypt as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAesDecrypt' using jar 'hdfs://CDM1D07-209022141.wdds.com:8020/user/zhaoxudong5/udf/aes.jar';

 create function aes_encrypt as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFAesEncrypt' using jar 'hdfs://CDM1D07-209022141.wdds.com:8020/user/zhaoxudong5/udf/aes.jar';
 
### How to build? ###
sudo docker run -ti --rm -v "$PWD:/app" -v "$HOME/.ivy2":/root/.ivy2 1science/sbt:0.13.8-oracle-jre-7 sbt package