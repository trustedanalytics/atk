#!/bin/bash
#
#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


#get the script path
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")

URL="trustedanalytics.com"
MAINTAINER="BDA <BDA@trustedanalytics.com>"

#deb build defaults
#some sensible defaults for some of the fields in all these control files
BUILD_DEPENDS="debhelper (>= 9.0.0)"
SUMMARY="$packageName Branch: $BRANCH version: $VERSION Build number: $BUILD_NUMBER. TimeStamp $TIMESTAMP"
DEPENDS="\${misc:Depends}"
STANDARDS_VERSION="3.9.3"
ARCH="any"
SECTION="libs"
PRIORITY="extra"
COMPAT=9

#rpm build defaults
PROVIDES=$PACKAGE_NAME
export DAY=$(date +"%Y%m%d")
RELEASE=$DAY$BUILD_NUMBER
SOURCE="$PACKAGE_NAME-$VERSION.tar.gz"

# Intel DAAL version
DAAL_JAR_VERSION="2016.0.109"

gitLog=$(git log -n 1)

function log()
{
 	echo "-##LOG##-$1"
}

function deleteOldBuildDirs()
{
	echo "delete old build dirs"
	echo "rm -rf $SCRIPTPATH/$PACKAGE_NAME-$VERSION"
	rm -rf $SCRIPTPATH/$PACKAGE_NAME-$VERSION
}

function tarFiles()
{
	mkdir TESTTAR
	: > FILES.LOG
	tar -xvf $1 -C TESTTAR > TAR.LOG
	if [ -f FILES.LOG ] && [ -f CONFIGFILES.LOG ]; then
	    rm FILES.LOG CONFIGFILES.LOG
	fi
	for path in `cat TAR.LOG`;
	do

		fullPath=$path
		fileName=${path##*/}
		log "filename $fileName"
		if [ "$fileName" != "" ]; then

			if [[ ! $fullPath == \/* ]]; then
				fullPath=${fullPath}
			fi


			if [[ $fullPath == */etc/* ]] && [[ $fullPath != */etc/init* ]]; then
                echo $fullPath | sed 's/^.\//\//g' >> CONFIGFILES.LOG
                else
                echo $fullPath | sed 's/^.\//\//g' >> FILES.LOG
			fi
		fi
	done
	rm -rf TESTTAR
	export TAR_FILES=FILES.LOG
	export TAR_FILES_CONFIG=CONFIGFILES.LOG
}

function expandTarDeb()
{
	packageTar=${packageName}_${version}.orig.tar.gz
	rm -rf $packageTar
	
	rm -rf $SCRIPTPATH/repack/
	log "repack source tar gz with packageName-version parent file"
	mkdir -p $SCRIPTPATH/repack/${packageName}-${version}
	
	tar -xvf $tarFile -C $SCRIPTPATH/repack/${packageName}-${version}

	pushd $SCRIPTPATH/repack/
	tar -pczf ../$packageTar  ${packageName}-${version}/
	popd
	
	log "untar"
	tar -xvf $SCRIPTPATH/${packageTar} -C $SCRIPTPATH
}

#the deb control is the deb packages meta data file it's kind of like the rpm spec file
function debControl()
{
	echo "Source: $SOURCE"
	echo "Priority: $PRIORITY"
	echo "Maintainer: $MAINTAINER"
	echo "Build-Depends: $BUILD_DEPENDS"
	echo "Standards-Version: $STANDARDS_VERSION"
	echo "Section: $SECTION"
	echo ""
	echo "Package: $PACKAGE_NAME"
	echo "Architecture: $ARCH"
	if [ ! -z "$DEPENDS" ]; then
		echo "Depends: $DEPENDS, \${misc:Depends}"
	fi
	if [ ! -z "$RECOMMENDS" ];then
		echo "Recommends: $RECOMMENDS"
	fi
	echo "Description: $SUMMARY"
	echo -e " $DESCRIPTION"
	#IFS="\n"
	#for $line in $gitLog
	#do
 	#echo  " $line"
	#done
}

function debCopyright()
{
	echo "TC package $BUILD_NUMBER $TIMESTAMP $MAINTAINER"
	echo ""
	echo "Copyright:"
	echo ""
	echo "Copyright (C) 2014 Intel Corporation"
	echo ""
	echo "License:"
	echo ""
	echo "All Rights reserved."
}

function debChangeLog()
{
	dch --create -M -v $version --package $packageName "Initial release. Closes: #XXXXXX"
}

#not much explanation is given for this file with a magical number for the time being it's  defaulted to 9
function debCompat()
{
	echo $COMPAT
}

#list of files that
function debInstall()
{
	for file in `cat $TAR_FILES`;
	do
		local fileName=${file##*/}

		installDir=$(echo $file | sed "s/$fileName/ /g")

		echo "$file $installDir"
	done
}

function debRules()
{
	if [ -z "$RULEOPT" ]; then
		RULEOPT=""
	fi
	echo "#!/usr/bin/make -f"
	echo "# Uncomment this to turn on verbose mode."
	if [ ! -z "$RULESSETUP" ]; then
		echo $RULESSETUP
	fi
	echo "#export DH_VERBOSE=1"
	echo "%:"
	echo -e "\tdh \$@ $RULEOPT"
}

function package(){
    packageName=$1

        log "package ${packageName}"
        config/$packageName/package.sh ${packageName}

}

function createArchive(){
    packageName=$1
    log "archive for $packageName dir: $BUILD_DIR"
    pushd ${BUILD_DIR}
    if [ $packageName == $PACKAGE_NAME ]; then
        log "tar zcvf ../$packageName-source.tar.gz . --owner=root --group=root"
        log "`pwd`"
        tar zcvf ../$packageName-source.tar.gz . --owner=root --group=root
        #tar -pczf ../$package-source.tar.gz .
    fi
    popd
}

function cleanBuild(){
    packageName=$1
    echo cleanBuildDir
    echo $packageName
    if [ -d ${BUILD_DIR}/${packageName} ]; then
        log "remove old build folder"
        rm -rf ${BUILD_DIR}/${packageName}
        else
        log "no old build folders"
    fi
    if [ -f ${BUILD_DIR}/${packageName}-source.tar.gz ]; then
        log "delete old source file "
        rm ${BUILD_DIR}/${packageName}-source.tar.gz
        else
        log "no old source file"
    fi
}

function cleanDeb()
{
    log "clean deb build dirs"
    rm -rf ${SCRIPTPATH}/${debDir}
    rm -rf ${SCRIPTPATH}/repack
}


function cleanRpm()
{
    log "clean rpm build dirs"
    rm -rf BUILD/
    rm -rf BUILDROOT/
}

function rpmSpec()
{

echo "Name: $PACKAGE_NAME"
#echo "Provides: $PROVIDES"
echo "Summary: $SUMMARY"
echo "License: $LICENSE"
echo "Version: $VERSION"
#echo "Serial: $RELEASE"
echo "Group: $GROUP"
if [ ! -z "$REQUIRES" ];then
	echo "Requires: $REQUIRES"
fi
if [ ! -z "$PREFIX" ];then
	echo "Prefix: $PREFIX"
fi

echo "Release: $RELEASE"
echo "Source: $SOURCE"
if [ ! -z "$URL" ]; then
	echo "URL: $URL"
fi
echo "%description"
echo -e $DESCRIPTION

echo "%define TIMESTAMP %(echo $TIMESTAMP)"
echo "%define TAR_FILE %(echo $TAR_FILE)"
echo "%build"
echo " cp %{TAR_FILE} %{_builddir}/files.tar.gz"

echo "%install"

echo " rm -rf %{buildroot}"

echo " mkdir -p %{buildroot}"

echo " mv files.tar.gz %{buildroot}/files.tar.gz"

echo " tar -xvf %{buildroot}/files.tar.gz -C %{buildroot}"

echo " rm %{buildroot}/files.tar.gz"

echo "%clean"

echo "%pre"
if [ ! -z "$PRE" ]; then
	echo "$PRE"
fi

echo "%post"
if [ ! -z "$POST" ]; then
	echo "$POST"
fi

echo "%preun"
if [ ! -z "$PREUN" ]; then
    echo "$PREUN"
fi

echo "%postun"
if [ ! -z "$POSTUN" ]; then
    echo "$POSTUN"
fi

echo "%files"
if [ ! -z "$FILES" ]; then
    echo "$FILES"
fi

cat $TAR_FILES

if [ ! -z "$CONFIG" ]; then
  echo "$CONFIG"
fi

for configFile in `cat ${TAR_FILES_CONFIG}`
do
    echo "%config ${configFile}"
done

}

