# Hadoop & Mapreduce

## Dependencies

If you want to bootstrap this project, you'll need:
- Linux environment (Others systems weren't tested).
- Java 11+
- Maven 3.6.3+

## Running

1 - Go into the ```application``` folder and run this code:

    mvn clean package shade:shade

2 - Execute:

**You need pass file and folder with arguments**.

    java -jar ./target/application-1.0-SNAPSHOT.jar ../sales.csv ../outFolder


## Reference

- https://www.guru99.com/create-your-first-hadoop-program.html