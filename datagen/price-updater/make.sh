mvn clean package
cp ./target/price-updater-1.0-jar-with-dependencies.jar ./price-updater-1.0.jar
if [ ! -f config.properties ] ; 
  then cp ./src/main/java/acme/config.properties . 
  chmod 600 config.properties
fi
