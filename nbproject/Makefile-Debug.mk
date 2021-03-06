#
# Generated Makefile - do not edit!
#
# Edit the Makefile in the project folder instead (../Makefile). Each target
# has a -pre and a -post target defined where you can add customized code.
#
# This makefile implements configuration specific macros and targets.


# Environment
MKDIR=mkdir
CP=cp
GREP=grep
NM=nm
CCADMIN=CCadmin
RANLIB=ranlib
CC=gcc
CCC=g++
CXX=g++
FC=gfortran
AS=as

# Macros
CND_PLATFORM=GNU-Linux-x86
CND_DLIB_EXT=so
CND_CONF=Debug
CND_DISTDIR=dist
CND_BUILDDIR=build

# Include project Makefile
include Makefile

# Object Directory
OBJECTDIR=${CND_BUILDDIR}/${CND_CONF}/${CND_PLATFORM}

# Object Files
OBJECTFILES= \
	${OBJECTDIR}/datastore_client.o \
	${OBJECTDIR}/datastore_receiver.o \
	${OBJECTDIR}/main.o \
	${OBJECTDIR}/publisher.o


# C Compiler Flags
CFLAGS=

# CC Compiler Flags
CCFLAGS=
CXXFLAGS=

# Fortran Compiler Flags
FFLAGS=

# Assembler Flags
ASFLAGS=

# Link Libraries and Options
LDLIBSOPTIONS=`pkg-config --libs librabbitmq` -lpthread  `pkg-config --libs liblog4cxx` `pkg-config --libs jsoncpp` irodsfs/libirodsfs.so  

# Build Targets
.build-conf: ${BUILD_SUBPROJECTS}
	"${MAKE}"  -f nbproject/Makefile-${CND_CONF}.mk ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/iplant_border_msg_server

${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/iplant_border_msg_server: irodsfs/libirodsfs.so

${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/iplant_border_msg_server: ${OBJECTFILES}
	${MKDIR} -p ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}
	${LINK.cc} -o ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/iplant_border_msg_server ${OBJECTFILES} ${LDLIBSOPTIONS}

${OBJECTDIR}/datastore_client.o: datastore_client.cpp 
	${MKDIR} -p ${OBJECTDIR}
	${RM} "$@.d"
	$(COMPILE.cc) -g `pkg-config --cflags librabbitmq` `pkg-config --cflags liblog4cxx` `pkg-config --cflags jsoncpp`   -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/datastore_client.o datastore_client.cpp

${OBJECTDIR}/datastore_receiver.o: datastore_receiver.cpp 
	${MKDIR} -p ${OBJECTDIR}
	${RM} "$@.d"
	$(COMPILE.cc) -g `pkg-config --cflags librabbitmq` `pkg-config --cflags liblog4cxx` `pkg-config --cflags jsoncpp`   -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/datastore_receiver.o datastore_receiver.cpp

${OBJECTDIR}/datastore_receiver.hpp.gch: datastore_receiver.hpp 
	${MKDIR} -p ${OBJECTDIR}
	${RM} "$@.d"
	$(COMPILE.cc) -g `pkg-config --cflags librabbitmq` `pkg-config --cflags liblog4cxx` `pkg-config --cflags jsoncpp`   -MMD -MP -MF "$@.d" -o "$@" datastore_receiver.hpp

${OBJECTDIR}/main.o: main.cpp 
	${MKDIR} -p ${OBJECTDIR}
	${RM} "$@.d"
	$(COMPILE.cc) -g `pkg-config --cflags librabbitmq` `pkg-config --cflags liblog4cxx` `pkg-config --cflags jsoncpp`   -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/main.o main.cpp

${OBJECTDIR}/publisher.o: publisher.cpp 
	${MKDIR} -p ${OBJECTDIR}
	${RM} "$@.d"
	$(COMPILE.cc) -g `pkg-config --cflags librabbitmq` `pkg-config --cflags liblog4cxx` `pkg-config --cflags jsoncpp`   -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/publisher.o publisher.cpp

# Subprojects
.build-subprojects:

# Clean Targets
.clean-conf: ${CLEAN_SUBPROJECTS}
	${RM} -r ${CND_BUILDDIR}/${CND_CONF}
	${RM} ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/iplant_border_msg_server

# Subprojects
.clean-subprojects:

# Enable dependency checking
.dep.inc: .depcheck-impl

include .dep.inc
