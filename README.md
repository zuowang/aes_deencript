### How to use? ###
cd aes-deencrypt
hive -i aes_init
hive>  hex(aes_encrypt('ABC', '1234567890123456'));
OK
CBA4ACFB309839BA426E07D67F23564F
hive> select aes_decrypt(unhex('CBA4ACFB309839BA426E07D67F23564F'), '1234567890123456');
OK
ABC