#!/bin/bash

# create new CA key and certificate
openssl req -new -newkey rsa:2048 -days 3650 -x509 -subj "/CN=root-ca/OU=drivers/O=oss/C=US" -keyout ca-key -out ca-cert -nodes

# create keystore and key-pair for DSE server
keytool -genkey -keyalg RSA -keystore 127.0.0.1.keystore -validity 3650 -storepass cassandra -keypass cassandra -dname "CN=127.0.0.1,OU=drivers,O=oss,C=US" -ext "SAN=IP:127.0.0.1" -alias 127.0.0.1 -storetype pkcs12

# export DSE server key from keystore
openssl pkcs12 -in 127.0.0.1.keystore -nodes -nocerts -out client.key -legacy -passin pass:cassandra

# create encrypted client key
openssl rsa -aes256 -in client.key -passout pass:cassandra -out client_encrypted.key

# create CSR
keytool -keystore 127.0.0.1.keystore -alias 127.0.0.1 -certreq -file client.csr -storepass cassandra -ext san=ip:127.0.0.1

# sign CSR with CA key
openssl x509 -req -CA ca-cert -CAkey ca-key -in client.csr -out client.crt -days 3650 -copy_extensions copyall -passin pass:cassandra

# import CA certificate to DSE node keystore
keytool -keystore 127.0.0.1.keystore -alias CARoot -import -file ca-cert -storepass cassandra -noprompt

# import signed certificate to DSE node keystore
keytool -keystore 127.0.0.1.keystore -alias 127.0.0.1 -import -file client.crt -storepass cassandra -noprompt

# import CA certificate to DSE node truststore
keytool -keystore cassandra.truststore -alias CARoot -import -file ca-cert -storepass cassandra -noprompt

# cleanup
rm client.csr