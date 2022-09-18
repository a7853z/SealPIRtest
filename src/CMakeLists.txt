find_package(SEAL 4.0 REQUIRED)

add_library(sealpir pir.hpp pir.cpp pir_client.hpp pir_client.cpp pir_server.hpp
  pir_server.cpp db_center.cpp common.cpp SHA256.cpp)
target_link_libraries(sealpir SEAL::seal)

add_executable(main main.cpp)
add_executable(server server.cpp config_file.cpp NetServer.cpp common.cpp)
add_executable(client client.cpp config_file.cpp NetClient.cpp common.cpp)
target_link_libraries(main sealpir)
target_link_libraries(server sealpir)
target_link_libraries(client sealpir)
