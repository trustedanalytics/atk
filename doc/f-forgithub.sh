echo $ModifiedArguments | grep -i -e "/-_/" > /dev/null
if [ "$?" != "0" ]; then
    echo $Format | grep -i -e "html" > /dev/null
    if [ "$?" == "0" ]; then
        if [[ -d "build/$Format" ]]; then
            rename_folders "$Format" $AskThem $DisplayStatus $TempFile $LogFile $ModifiedArguments
            if [ "$?" == "1" ]; then return 1; fi
        fi
    fi
fi
