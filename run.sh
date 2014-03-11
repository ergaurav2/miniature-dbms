javac -cp ".:201305574_src/gsp.jar" 201305574_src/*.java
java -cp "201305574_src:201305574_src/gsp.jar" DBSystem "$1"
