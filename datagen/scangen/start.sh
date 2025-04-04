if [ `uname -s` = "Linux" ]; then
  sudo timedatectl set-timezone America/Los_Angeles
fi
java -jar scangen-1.1.jar
