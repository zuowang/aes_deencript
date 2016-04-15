### How to use? ###
cd aes-deencrypt

hive -i 

hive>  hex(aes_encrypt('ABC', '1234567890123456'));

OK

CBA4ACFB309839BA426E07D67F23564F

hive> select aes_decrypt(unhex('CBA4ACFB309839BA426E07D67F23564F'), '1234567890123456');

OK

ABC

### How to build? ###
sudo docker run -ti --rm -v "$PWD:/app" -v "$HOME/.ivy2":/root/.ivy2 1science/sbt:0.13.8-oracle-jre-7 sbt package