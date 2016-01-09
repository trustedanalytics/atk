#
# 'make'        build dynamic library 'libIaDaalJavaAPI.so'
# 'make clean'  removes all .o and executable files
#

# define the C compiler to use
CC = g++

# define any compile-time flags
CFLAGS = -c -fPIC

# define any directories containing header files other than /usr/include
#
INCLUDES = -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I${DAALROOT}/include

# define library paths in addition to /usr/lib
LFLAGS  = -L${DAALROOT}/lib/intel64_lin
LDFLAGS = -shared

TARGET  = libAtkDaalJavaAPI.so
TARGETFILE = ../../../lib/intel64_lin/${TARGET}
SOURCES = com_intel_daal_algorithms_ModelSerializer.c
OBJ     = serializer.o

all:    $(TARGET)
	@echo  Compiling IA DAAL library

${OBJ} : ${SOURCES}
	$(CC) $(CFLAGS) $(INCLUDES) -o ${OBJ} $(LFLAGS) ${SOURCES}

${TARGET} : serializer.o
	g++ -shared -Wl,-soname,${TARGET} ${LFLAGS} -o ${TARGETFILE} ${OBJ} -lJavaAPI

clean:
	$(RM) *.o ${TARGETFILE}

install: ${TARGET}
	sudo cp ${TARGETFILE} ${DAALROOT}/lib/intel64_lin/

# DO NOT DELETE THIS LINE -- make depend needs it
