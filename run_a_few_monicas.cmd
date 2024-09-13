set PATH_TO_MONICA_BIN_DIR=C:\Users\berg\GitHub\monica\_cmake_release
set MONICA_PARAMETERS=C:\Users\berg\GitHub\monica-parameters
echo "MONICA_PARAMETERS=%MONICA_PARAMETERS%"

START "ZMQ_IN_PROXY" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-proxy -pps -f 6666 -b 6677 &
START "ZMQ_OUT_PROXY" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-proxy -pps -f 7788 -b 7777 &

FOR /L %%I IN (1,1,6) DO (
  START "MONICA_%%I" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
)
