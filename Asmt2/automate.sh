echo "Compiling for:"$1
find . -name '*.class' -exec rm -f {} \;
find . -name '*.jar' -exec rm -f {} \;
javac -classpath `hadoop classpath` $1 && echo "[SCRIPT OUTPUT]Compilation Sucessfull" || echo "[SCRIPT OUTPUT]Compilation Failed"
temp="*.class"
jar cf $2 $temp&& echo "[SCRIPT OUTPUT]JAR Creation Sucessfull" || echo "[SCRIPT OUTPUT]JAR Creation Failed"
find . -name '*.class' -exec rm -f {} \;
