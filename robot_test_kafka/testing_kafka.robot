*** settings ***
Library         ./TestKafka/TestKafka.py

*** Test Cases ***
Create Topic
    create topic    waseem4
    topic should exist    waseem4

Delete Topic
    create topic    waseem55
    topic should exist    waseem55
    delete topic            waseem55
    topic should not exist  waseem55

Send File To topic
    send file to kafka topic    waseem4     ./Resources/input2.txt

Read Kafka Topic
    kafka Consumer  waseem4     output.txt

Get All Topics
    ${Topics}   List All Topics
    Log         ${Topics}