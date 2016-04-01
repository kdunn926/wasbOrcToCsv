CC=/opt/rh/devtoolset-3/root/usr/bin/gcc 
CXX=/opt/rh/devtoolset-3/root/usr/bin/g++
INCLUDES=-I /opt/microsquid/include -I /opt/apache/orc/include
LD_FLAGS=-lcrypto -lssl -lboost_system -l cpprest -l azurestorage -l orc -L /opt/microsquid/lib -L /opt/apache/orc/lib /opt/apache/orc/lib/libz.a /opt/apache/orc/lib/libprotobuf.a /opt/apache/orc/lib/libsnappy.a

all: w2o.o
	${CXX} -std=c++11 w2o.o -o w2o ${LD_FLAGS}

w2o.o:
	${CXX} -c -std=c++11 wasbToOrc.cpp ${INCLUDES} -o w2o.o

clean:
	rm w2o.o w2o
