if [ ! -f config.properties ] ; 
  then cp config.properties.example config.properties
  chmod 600 config.properties
  echo "Please edit config.properties as needed before running the program" 
fi

if [ `uname -s` = "Linux" ]; then
  sudo timedatectl set-timezone America/Los_Angeles
fi
java -jar scangen-1.1.jar
