mvn clean package
cp ./target/scangen-1.1-jar-with-dependencies.jar ./scangen-1.1.jar
if [ ! -f config.properties ] ; 
  then cp config.properties.example config.properties
  chmod 600 config.properties
  echo "Please edit config.properties as needed before running the program" 
fi
