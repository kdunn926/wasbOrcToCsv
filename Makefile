CC=/opt/rh/devtoolset-3/root/usr/bin/gcc
CXX=/opt/rh/devtoolset-3/root/usr/bin/g++
LD_FLAGS=-lxml2 -lz -lcrypto -lssl -lboost_system -lcpprest -lazurestorage -lorc /usr/lib/libsnappy.a /usr/lib/libprotobuf.a /usr/lib/libz.a

all: w2o wTd

w2o: w2o.o
	${CXX} -std=c++11 w2o.o -o w2o ${LD_FLAGS}

w2o.o:
	${CXX} -c -std=c++11 wasbToOrc.cpp ${INCLUDES} -o w2o.o

wTd: wTd.o
	${CXX} -std=c++11 wTd.o -o wTd ${LD_FLAGS}

wTd.o:
	${CXX} -c -std=c++11 wasbTextDump.cpp ${INCLUDES} -o wTd.o

clean:
	rm -f w2o.o w2o wTd.o wTd
