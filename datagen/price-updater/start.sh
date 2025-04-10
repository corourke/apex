if [ ! -f config.properties ] ; 
  then cp config_example.properties config.properties
  chmod 600 config.properties
  echo "Please edit config.properties as needed before running the program" 
  exit
fi
if [ `uname -s` = "Linux" ]; then
  sudo timedatectl set-timezone America/Los_Angeles
fi
java -jar price-updater-1.0.jar
