```shell
git clone https://github.com/googleapis/api-common-protos.git
protoc -I=./protobuf/ --java_out=./src/main/java market_data.proto
```