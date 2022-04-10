FROM openjdk:11 as builder
WORKDIR /build
RUN apt update && apt install -y maven
COPY . . 
RUN mvn -DskipTests package 

FROM openjdk:11	
WORKDIR /app
COPY --from=builder /build/target/restorage-1.0-jar-with-dependencies.jar /app/restorage-1.0-jar-with-dependencies.jar
RUN mkdir /data
CMD java -cp restorage-1.0-jar-with-dependencies.jar com.github.shahrivari.restorage.ReStorageAppKt --port 8000 --root /data
