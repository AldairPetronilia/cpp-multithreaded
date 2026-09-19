# Converts INPUT into a comma-separated list of byte literals that a C++ source file can
# #include inside an array initialiser. Run with: cmake -DINPUT=... -DOUTPUT=... -P <this>
file(READ "${INPUT}" content HEX)
string(REGEX REPLACE "([0-9a-f][0-9a-f])" "0x\\1," bytes "${content}")
file(WRITE "${OUTPUT}" "${bytes}\n")
